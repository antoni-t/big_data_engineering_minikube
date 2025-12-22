#!/usr/bin/env bash

set -euo pipefail

######################################################################
# Bootstrap-Script für Big Data Engineering auf Minikube
# - Stellt sicher, dass Minikube läuft
# - Schaltet sinnvolle Addons frei (metrics-server, ingress)
# - Installiert Strimzi + Kafka (my-cluster)
# - Baut und deployed den Weather Producer (Deployment)
# - Baut ein Scraper-Image und legt zwei CronJobs an:
#     * autobahn-scraper (alle 30 Minuten)
#     * wetter-scraper   (jede Stunde)
# - Installiert ArgoCD und registriert eine Root-Application
######################################################################

# Konfiguration (bei Bedarf anpassen)
GIT_REPO_URL="https://github.com/antoni-t/big_data_engineering_minikube.git"
GIT_BRANCH="${GIT_BRANCH:-main}"            # ggf. auf "master" anpassen
ARGO_APP_NAME="${ARGO_APP_NAME:-big-data-engineering}"
ARGO_APP_NAMESPACE="argocd"
ARGO_APP_DEST_NAMESPACE="default"          # Ziel-Namespace für Deployments (oder z.B. "bigdata")
ARGO_APP_DEST_SERVER="https://kubernetes.default.svc"
# Pfad im Git-Repo, in dem deine ArgoCD-App-Definitionen liegen (z.B. App-of-Apps)
ARGO_APP_PATH="${ARGO_APP_PATH:-argocd/apps}"

# Basisverzeichnis (Repo-Root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRODUCER_DIR="${SCRIPT_DIR}/weather-producer"
SCRAPER_DIR="${SCRIPT_DIR}/scraper"

echo "=== Big Data Engineering Bootstrap startet ==="

######################################################################
# 1. Minikube prüfen / starten
######################################################################
if ! command -v minikube >/dev/null 2>&1; then
  echo "Fehler: minikube ist nicht installiert oder nicht im PATH."
  exit 1
fi

if ! command -v kubectl >/dev/null 2>&1; then
  echo "Fehler: kubectl ist nicht installiert oder nicht im PATH."
  exit 1
fi

if ! command -v helm >/dev/null 2>&1; then
  echo "Fehler: helm ist nicht installiert oder nicht im PATH."
  exit 1
fi

echo "Prüfe Minikube-Status…"
if ! minikube status >/dev/null 2>&1; then
  echo "Minikube läuft nicht – starte Minikube mit Docker-Driver."
  minikube start --driver=docker
else
  echo "Minikube ist bereits gestartet."
fi

# Docker-Umgebung auf Minikube umstellen (für spätere Image-Builds)
echo "Setze Docker-Umgebung auf Minikube…"
eval "$(minikube docker-env)"

######################################################################
# 1b) Sinnvolle Addons aktivieren (metrics-server, ingress)
######################################################################
echo "Aktiviere Minikube-Addons (metrics-server, ingress)…"

if ! minikube addons list | grep -q "metrics-server.*enabled"; then
  echo "- metrics-server aktivieren"
  minikube addons enable metrics-server
else
  echo "- metrics-server bereits aktiviert"
fi

if ! minikube addons list | grep -q "ingress[[:space:]]*enabled"; then
  echo "- ingress aktivieren"
  minikube addons enable ingress
else
  echo "- ingress bereits aktiviert"
fi

######################################################################
# 2. Strimzi / Kafka-Cluster (Namespace: kafka)
######################################################################
echo "Richte Strimzi / Kafka ein…"

# Namespace für Strimzi + Kafka
if ! kubectl get ns kafka >/dev/null 2>&1; then
  echo "Erzeuge Namespace 'kafka'…"
  kubectl create namespace kafka
else
  echo "Namespace 'kafka' existiert bereits."
fi

######################################################################
# 2a. Strimzi-Operator installieren (im Namespace kafka)
######################################################################
echo "Installiere / aktualisiere Strimzi-Operator im Namespace 'kafka'…"

kubectl apply -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka

echo "Warte, bis Strimzi-Operator bereit ist…"
kubectl rollout status deployment/strimzi-cluster-operator -n kafka --timeout=300s

######################################################################
# 2b. Kafka Single-Node Cluster deployen (my-cluster)
######################################################################
echo "Erzeuge/aktualisiere Kafka-Cluster 'my-cluster' (Single-Node)…"

kubectl apply -f "https://strimzi.io/examples/latest/kafka/kafka-ephemeral.yaml" -n kafka

echo "Warte, bis Kafka-Cluster 'my-cluster' bereit ist…"
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=600s -n kafka || {
  echo "WARNUNG: Kafka-Cluster 'my-cluster' wurde nicht rechtzeitig Ready."
}

######################################################################
# 2c. Kafka-Topic 'weather-raw' anlegen (für Weather Producer)
######################################################################
echo "Erzeuge/aktualisiere KafkaTopic 'weather-raw'…"

cat <<EOF | kubectl apply -n kafka -f -
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: weather-raw
  labels:
    strimzi.io/cluster: my-cluster
spec:
  partitions: 3
  replicas: 1
EOF

echo "KafkaTopic 'weather-raw' wurde angewendet."
echo "Strimzi / Kafka-Setup abgeschlossen."
echo "Kafka-Bootstrap-Address (intern): my-cluster-kafka-bootstrap.kafka.svc:9092"

######################################################################
# 3. Docker-Image für Weather Producer bauen
######################################################################
echo "Baue lokales Docker-Image 'weather-producer:latest' ..."

if [ ! -d "${PRODUCER_DIR}" ]; then
  echo "FEHLER: Verzeichnis ${PRODUCER_DIR} existiert nicht. Bitte prüfe deine Projektstruktur."
  exit 1
fi

cd "${PRODUCER_DIR}"

if [ ! -f "Dockerfile" ]; then
  echo "FEHLER: Dockerfile im Verzeichnis ${PRODUCER_DIR} nicht gefunden!"
  exit 1
fi

if [ ! -f "weather_kafka_producer.py" ]; then
  echo "FEHLER: weather_kafka_producer.py im Verzeichnis ${PRODUCER_DIR} nicht gefunden!"
  exit 1
fi

docker build -t weather-producer:latest .

echo "Docker-Image 'weather-producer:latest' wurde erfolgreich gebaut."

# Zurück ins ursprüngliche Verzeichnis (Repo-Root)
cd "${SCRIPT_DIR}"

######################################################################
# 4. Kubernetes Deployment für Weather Producer anlegen/aktualisieren
######################################################################
echo "Erzeuge/aktualisiere Deployment 'weather-producer' ..."

cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: weather-producer
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: weather-producer
  template:
    metadata:
      labels:
        app: weather-producer
    spec:
      containers:
        - name: weather-producer
          image: weather-producer:latest
          imagePullPolicy: Never   # wichtig: nutze lokales Minikube-Image
          env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "my-cluster-kafka-bootstrap.kafka.svc:9092"
            - name: KAFKA_TOPIC
              value: "weather-raw"
            - name: GRID_PATH
              value: "de_grid_cell_centers.csv"
            - name: POLL_INTERVAL_SECONDS
              value: "3600"
          resources:
            requests:
              cpu: "100m"
              memory: "256Mi"
            limits:
              cpu: "500m"
              memory: "512Mi"
EOF

echo "Deployment 'weather-producer' wurde angewendet."

######################################################################
# 5. Scraper-Image bauen (autobahn + wetter)
######################################################################
echo "Baue lokales Docker-Image 'scraper:latest' ..."

if [ ! -d "${SCRAPER_DIR}" ]; then
  echo "FEHLER: Verzeichnis ${SCRAPER_DIR} existiert nicht. Bitte prüfe deine Projektstruktur."
  exit 1
fi

cd "${SCRAPER_DIR}"

if [ ! -f "Dockerfile" ]; then
  echo "FEHLER: Dockerfile im Verzeichnis ${SCRAPER_DIR} nicht gefunden!"
  exit 1
fi

docker build -t scraper:latest .

echo "Docker-Image 'scraper:latest' wurde erfolgreich gebaut."
cd "${SCRIPT_DIR}"

######################################################################
# 6. CronJobs für Autobahn- und Wetter-Scraper anlegen
######################################################################
echo "Erzeuge/aktualisiere CronJob 'autobahn-scraper' (alle 30 Minuten) ..."
cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: CronJob
metadata:
  name: autobahn-scraper
  namespace: default
spec:
  schedule: "*/30 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
            - name: autobahn-scraper
              image: scraper:latest
              imagePullPolicy: Never
              env:
                - name: SCRAPER_DATA_DIR
                  value: "/data"
              command: ["python", "autobahn_scraper.py"]
              volumeMounts:
                - name: scraper-data
                  mountPath: /data
          volumes:
            - name: scraper-data
              emptyDir: {}
EOF

echo "Erzeuge/aktualisiere CronJob 'wetter-scraper' (stündlich) ..."
cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: CronJob
metadata:
  name: wetter-scraper
  namespace: default
spec:
  schedule: "0 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
            - name: wetter-scraper
              image: scraper:latest
              imagePullPolicy: Never
              env:
                - name: SCRAPER_DATA_DIR
                  value: "/data"
              command: ["python", "wetter_scraper.py"]
              volumeMounts:
                - name: scraper-data
                  mountPath: /data
          volumes:
            - name: scraper-data
              emptyDir: {}
EOF

echo "CronJobs 'autobahn-scraper' und 'wetter-scraper' wurden angewendet."

######################################################################
# 7. ArgoCD installieren (falls nicht vorhanden)
######################################################################
echo "Prüfe ArgoCD-Installation…"

if ! kubectl get ns "${ARGO_APP_NAMESPACE}" >/dev/null 2>&1; then
  echo "Namespace ${ARGO_APP_NAMESPACE} existiert nicht – lege ihn an und installiere ArgoCD…"
  kubectl create namespace "${ARGO_APP_NAMESPACE}"

  kubectl apply -n "${ARGO_APP_NAMESPACE}" \
    -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

  echo "Warte, bis ArgoCD Deployments verfügbar sind…"
  kubectl wait --for=condition=available --timeout=600s deployment \
    -l app.kubernetes.io/part-of=argocd -n "${ARGO_APP_NAMESPACE}"
else
  echo "ArgoCD Namespace ${ARGO_APP_NAMESPACE} existiert bereits."
fi

######################################################################
# 8. ArgoCD Root-Application anlegen (GitOps-Einstiegspunkt)
######################################################################
echo "Erzeuge/aktualisiere ArgoCD Application '${ARGO_APP_NAME}'…"

cat <<EOF | kubectl apply -n "${ARGO_APP_NAMESPACE}" -f -
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: ${ARGO_APP_NAME}
  namespace: ${ARGO_APP_NAMESPACE}
  finalizers:
    - resources-finalizer.argocd.argoproj.io
spec:
  project: default
  source:
    repoURL: ${GIT_REPO_URL}
    targetRevision: ${GIT_BRANCH}
    path: ${ARGO_APP_PATH}
  destination:
    server: ${ARGO_APP_DEST_SERVER}
    namespace: ${ARGO_APP_DEST_NAMESPACE}
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
EOF

echo "ArgoCD Application '${ARGO_APP_NAME}' wurde angewendet."
echo "Stelle sicher, dass im Git-Repo unter Pfad '${ARGO_APP_PATH}' passende ArgoCD-Manifeste (z.B. App-of-Apps) liegen."

######################################################################
# 9. Optional: Port-Forward für ArgoCD-UI
######################################################################
echo "Richte optionales Port-Forward für ArgoCD-Weboberfläche ein…"

# Alte port-forwards beenden (Linux/Mac)
if command -v pkill >/dev/null 2>&1; then
  pkill -f "kubectl port-forward .*argocd-server" || true
fi

# Windows: kubectl.exe beenden, falls vorhanden
if command -v taskkill >/dev/null 2>&1; then
  taskkill /IM kubectl.exe /F 2>nul || true
fi

# Neues Port-Forward einrichten
kubectl port-forward svc/argocd-server -n "${ARGO_APP_NAMESPACE}" 8080:443 >/dev/null 2>&1 &

echo "=================================================================="
echo "Bootstrap abgeschlossen."
echo
echo "ArgoCD-UI: https://localhost:8080"
echo "Standard-Login (wenn noch nicht geändert):"
echo "  Benutzername: admin"
echo "  Passwort:     kubectl -n argocd get secret argocd-initial-admin-secret \\"
echo "                 -o jsonpath='{.data.password}' | base64 -d && echo"
echo
echo "Deployments:"
echo "  - Kafka/Strimzi im Namespace 'kafka'"
echo "  - Weather Producer (Deployment) im Namespace 'default'"
echo "  - CronJobs 'autobahn-scraper' & 'wetter-scraper' im Namespace 'default'"
echo "  - ArgoCD im Namespace '${ARGO_APP_NAMESPACE}'"
echo "=================================================================="

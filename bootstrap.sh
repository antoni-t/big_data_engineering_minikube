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
MACHINE_LEARNING_DIR="${SCRIPT_DIR}/machine_learning_model"

# Lokale Trainingsdaten (Windows) -> in Minikube mounten
AUTOBAHN_LOCAL_DIR="${AUTOBAHN_LOCAL_DIR:-C:/Users/thoma/DHBW_master_semester_3/big_data_engineering/Gruppen-Projekt/ML_training_final/Alle-Autobahn-Daten/autobahn}"
WEATHER_LOCAL_DIR="${WEATHER_LOCAL_DIR:-C:/Users/thoma/DHBW_master_semester_3/big_data_engineering/Gruppen-Projekt/ML_training_final/weather_data}"

# Zielpfade in Minikube VM
AUTOBAHN_MK_DIR="/mnt/autobahn_data"
WEATHER_MK_DIR="/mnt/weather_hist"


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
  minikube start --driver=docker --cpus=4 --memory=10240 
else
  echo "Minikube ist bereits gestartet."
fi

# Docker-Umgebung auf Minikube umstellen (für spätere Image-Builds)
echo "Setze Docker-Umgebung auf Minikube…"
eval "$(minikube docker-env)"

echo "Mount lokale Trainingsdaten in Minikube (hostPath Quelle)..."
echo "AUTOBAHN_LOCAL_DIR=${AUTOBAHN_LOCAL_DIR}"
echo "WEATHER_LOCAL_DIR=${WEATHER_LOCAL_DIR}"
echo "HINWEIS: minikube mount läuft als Prozess im Hintergrund."

# Windows Git-Bash: Path-Konvertierung vermeiden
MSYS_NO_PATHCONV=1 nohup minikube mount "${AUTOBAHN_LOCAL_DIR}:${AUTOBAHN_MK_DIR}" >/tmp/minikube_mount_autobahn.log 2>&1 &
MSYS_NO_PATHCONV=1 nohup minikube mount "${WEATHER_LOCAL_DIR}:${WEATHER_MK_DIR}"   >/tmp/minikube_mount_weather.log  2>&1 &

sleep 60
echo "Mount-Prozesse gestartet. (Logs: /tmp/minikube_mount_autobahn.log /tmp/minikube_mount_weather.log)"

######################################################################
# 2. Sinnvolle Addons aktivieren (metrics-server, ingress)
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
# 3. Strimzi / Kafka-Cluster (Namespace: kafka)
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
# 4. Strimzi-Operator installieren (im Namespace kafka)
######################################################################
echo "Installiere / aktualisiere Strimzi-Operator im Namespace 'kafka'…"

kubectl apply -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka

echo "Warte, bis Strimzi-Operator bereit ist…"
kubectl rollout status deployment/strimzi-cluster-operator -n kafka --timeout=300s

######################################################################
# 5. Kafka Single-Node Cluster deployen (my-cluster)
######################################################################
echo "Erzeuge/aktualisiere Kafka-Cluster 'my-cluster' (Single-Node)…"

kubectl apply -f "https://strimzi.io/examples/latest/kafka/kafka-ephemeral.yaml" -n kafka

echo "Warte, bis Kafka-Cluster 'my-cluster' bereit ist…"
kubectl wait kafka/my-cluster --for=condition=Ready --timeout=600s -n kafka || {
  echo "WARNUNG: Kafka-Cluster 'my-cluster' wurde nicht rechtzeitig Ready."
}

######################################################################
# 6. Kafka-Topic 'weather-raw' anlegen (für Weather Producer)
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
# 7. MariaDB deployen
######################################################################
echo "Deploye MariaDB..."
kubectl apply -f "${SCRIPT_DIR}/mariadb/k8s/mariadb.yaml"
kubectl rollout status deploy/mariadb -n default --timeout=180s

######################################################################
# 8. Docker-Image für Machine Learning Model (Jupyter) bauen
######################################################################
echo "Baue lokales Docker-Image 'machine-learning-model:latest' ..."

if [ ! -d "${MACHINE_LEARNING_DIR}" ]; then
  echo "FEHLER: Verzeichnis ${MACHINE_LEARNING_DIR} existiert nicht."
  exit 1
fi

cd "${MACHINE_LEARNING_DIR}"

if [ ! -f "Dockerfile" ]; then
  echo "FEHLER: Dockerfile im Verzeichnis ${MACHINE_LEARNING_DIR} nicht gefunden!"
  exit 1
fi

docker build -t machine-learning-model:latest .

echo "Docker-Image 'machine-learning-model:latest' wurde erfolgreich gebaut."
cd "${SCRIPT_DIR}"

######################################################################
# 9. Scraper-Image bauen (autobahn + wetter)
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
# 10. Docker-Image für jupyter-consumer bauen
######################################################################
echo "Baue lokales Docker-Image 'jupyter-consumer:latest' ..."

JUPYTER_CONSUMER_DIR="${SCRIPT_DIR}/jupyter-consumer"

if [ ! -d "${JUPYTER_CONSUMER_DIR}" ]; then
  echo "FEHLER: Verzeichnis ${JUPYTER_CONSUMER_DIR} existiert nicht."
  exit 1
fi

cd "${JUPYTER_CONSUMER_DIR}"

if [ ! -f "Dockerfile" ]; then
  echo "FEHLER: Dockerfile im Verzeichnis ${JUPYTER_CONSUMER_DIR} nicht gefunden!"
  exit 1
fi

docker build -t jupyter-consumer:latest .

echo "Docker-Image 'jupyter-consumer:latest' wurde erfolgreich gebaut."
cd "${SCRIPT_DIR}"


######################################################################
# 11. Docker-Image für Weather Producer bauen
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
# 12. MLflow PVC + Training Job ausführen (RUN_ID erzeugen)
######################################################################
echo "Deploye MLflow PVC + starte Training Job..."

kubectl apply -f "${MACHINE_LEARNING_DIR}/k8s/mlflow-pvc.yaml"

# Job neu erstellen: wenn er schon existiert -> delete & recreate
kubectl delete job ml-train -n default >/dev/null 2>&1 || true
kubectl apply -f "${MACHINE_LEARNING_DIR}/k8s/train-job.yaml"

echo "Warte auf Abschluss des Training Jobs (ml-train)..."
kubectl wait --for=condition=complete job/ml-train -n default --timeout=900s || {
  echo "TRAINING JOB FAILED. Logs:"
  kubectl logs -n default job/ml-train --tail=200 || true
  exit 1
}

echo "Training Job completed."


######################################################################
# 13. jupyter-consumer deployen (startet erst wenn RUN_ID file da ist)
######################################################################

# jupyter-consumer deployen (hat den PVC gemountet)
echo "Deploye jupyter-consumer..."
kubectl apply -f "${SCRIPT_DIR}/jupyter-consumer/k8s/jupyter-consumer-deployment.yaml"
kubectl rollout status deployment/jupyter-consumer -n default --timeout=300s
kubectl apply -f "${SCRIPT_DIR}/jupyter-consumer/k8s/jupyter-consumer-hpa.yaml"

echo "jupyter-consumer deployed."

# RUN_ID über den laufenden consumer auslesen (kein exec in Completed Pod)
echo "Prüfe RUN_ID im PVC über jupyter-consumer:"
MSYS_NO_PATHCONV=1 kubectl exec -n default deploy/jupyter-consumer -- sh -lc \
  'ls -lah /mlruns && echo "RUN_ID:" && cat /mlruns/LATEST_RUN_ID'


######################################################################
# 14. Kubernetes Deployment für Weather Producer anlegen/aktualisieren
######################################################################

echo "Erzeuge/aktualisiere Deployment 'weather-producer' ..."

kubectl apply -f "${SCRIPT_DIR}/weather-producer/k8s/deployment-weather-producer.yaml"

echo "Warte auf Weather Producer Rollout..."
kubectl rollout status deploy/weather-producer -n default --timeout=180s

echo "Deployment 'weather-producer' ist Ready."


######################################################################
# 15. Heatmap (Jupyter) deployen
######################################################################

echo "Baue lokales Docker-Image 'heatmap:latest' ..."
docker build -t heatmap:latest "${SCRIPT_DIR}/heatmap"
echo "Heatmap Image gebaut."

echo "Deploye Heatmap (Jupyter)..."
kubectl apply -f "${SCRIPT_DIR}/heatmap/k8s/heatmap-jupyter.yaml"
kubectl apply -f "${SCRIPT_DIR}/heatmap/k8s/heatmap-service.yaml"

echo "Warte auf Heatmap Rollout..."
if ! kubectl rollout status deploy/heatmap -n default --timeout=600s; then
  echo "ERROR: Heatmap rollout failed. Debug info:"
  kubectl get pods -l app=heatmap -o wide -n default || true
  kubectl describe pod -l app=heatmap -n default || true
  POD=$(kubectl get pod -l app=heatmap -n default -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [ -n "$POD" ]; then
    kubectl logs -n default "$POD" --tail=200 || true
    kubectl logs -n default "$POD" --previous --tail=200 || true
  fi
  exit 1
fi

######################################################################
# 16. CronJobs für Autobahn- und Wetter-Scraper anlegen
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
# 17. Sofortige Initial-Ausführung (Kickoff Jobs)
######################################################################
echo "Starte beide Scraper sofort einmalig (Kickoff Jobs)..."

TS="$(date +%Y%m%d%H%M%S)"

AUTO_JOB="autobahn-scraper-bootstrap-${TS}"
WETTER_JOB="wetter-scraper-bootstrap-${TS}"

# Jobs nur erstellen, wenn es sie noch nicht gibt (idempotent)
kubectl get job "${AUTO_JOB}" -n default >/dev/null 2>&1 || \
  kubectl create job --from=cronjob/autobahn-scraper "${AUTO_JOB}" -n default

kubectl get job "${WETTER_JOB}" -n default >/dev/null 2>&1 || \
  kubectl create job --from=cronjob/wetter-scraper "${WETTER_JOB}" -n default

echo "Kickoff Jobs erstellt: ${AUTO_JOB}, ${WETTER_JOB}"

######################################################################
# 18. ArgoCD installieren (falls nicht vorhanden)
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
# 19. ArgoCD Root-Application anlegen (GitOps-Einstiegspunkt)
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
# 20. Optional: Port-Forward für ArgoCD-UI
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

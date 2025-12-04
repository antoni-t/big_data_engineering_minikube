#!/usr/bin/env bash

set -euo pipefail

######################################################################
# Bootstrap-Script für Big Data Engineering auf Minikube
# - Stellt sicher, dass Minikube läuft
# - Schaltet sinnvolle Addons frei (metrics-server, ingress)
# - Installiert ArgoCD (falls nicht vorhanden)
# - Registriert ein Root-Application-Objekt, das auf
#   https://github.com/antoni-t/big_data_engineering_minikube zeigt
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

# Docker-Umgebung auf Minikube umstellen (für spätere Image-Builds, falls benötigt)
echo "Setze Docker-Umgebung auf Minikube…"
eval "$(minikube docker-env)"

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
# 3. ArgoCD installieren (falls nicht vorhanden)
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
# 4. ArgoCD Root-Application anlegen (GitOps-Einstiegspunkt)
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
# 5. Optional: Port-Forward für ArgoCD-UI
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
echo "Die eigentlichen Deployments (Kafka/Strimzi, Jupyter, HDFS, Iceberg, etc.)"
echo "werden jetzt durch ArgoCD aus dem Git-Repo ${GIT_REPO_URL} synchronisiert."
echo "=================================================================="

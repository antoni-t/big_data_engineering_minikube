

# Big Data Engineering Autobahn-Forecast

## Einrichtungshinweise

### Voraussetzungen
- Minikube lokal installiert und konfiguriert
- Docker installiert
- kubectl installiert

### Erste Schritte

1. **Minikube starten**
    ```bash
    minikube start
    ```

2. **Bootstrap-Skript ausführen**
    ```bash
    ./bootstrap.sh
    ```

Das Bootstrap-Skript wird automatisch:
- Alle erforderlichen Abhängigkeiten für jeden Pod herunterladen
- Notwendige Docker-Images erstellen
- Alle Pods in Ihrem Minikube-Cluster bereitstellen und starten

### Überprüfung

Den Status der Pods überprüfen:
```bash
kubectl get pods
```

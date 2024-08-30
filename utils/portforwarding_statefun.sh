kubectl port-forward svc/statefun-master-rest 8081:8081 -n statefun & \
kubectl port-forward service/kafka-cluster-kafka-external-bootstrap 9093:9093 & \
kubectl port-forward deployment/prometheus-grafana 3000 &
kubectl port-forward service/prometheus-operated 9090:9090 &

echo "Press CTRL-C to stop port forwarding and exit the script"
wait
kubectl port-forward service/flink-session-cluster-rest 8081:8081 & \
kubectl port-forward service/kafka-cluster-kafka-external-bootstrap 9093:9093 & \
kubectl port-forward deployment/prometheus-grafana 3000 &
#kubectl port-forward svc/prometheus-kube-prometheus-prometheus 9090 &


echo "Press CTRL-C to stop port forwarding and exit the script"
wait
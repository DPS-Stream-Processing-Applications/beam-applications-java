kubectl port-forward service/session-cluster-rest 8081:8081 & \
kubectl port-forward service/kafka-cluster-kafka-external-bootstrap 9093:9093 &

echo "Press CTRL-C to stop port forwarding and exit the script"
wait

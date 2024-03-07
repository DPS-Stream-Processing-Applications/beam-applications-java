package at.ac.uibk.dps.streamprocessingapplications.tasks;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;

public class MQTTSubscribeTask extends AbstractTask implements MqttCallback {
    private static final Object SETUP_LOCK = new Object();
    private static boolean doneSetup = false;
    private static int useMsgField;
    private static String apolloUserName;
    private static String apolloPassword;
    private static String apolloURLlist;

    private static long counter = 1;

    private static String topic;

    /* local fields assigned to each thread */
    private MqttClient mqttClient;
    private String apolloClient;
    private String apolloURL;

    public LinkedBlockingQueue<String> incoming = new LinkedBlockingQueue<>(); // added later

    public MQTTSubscribeTask() throws MqttException {}

    public void setup(Logger l_, Properties p_) {
        super.setup(l_, p_);
        try {
            synchronized (SETUP_LOCK) {
                if (!doneSetup) {
                    useMsgField =
                            Integer.parseInt(p_.getProperty("IO.MQTT_SUBSCRIBE.USE_MSG_FIELD"));
                    apolloUserName = p_.getProperty("IO.MQTT_SUBSCRIBE.APOLLO_USER");
                    apolloPassword = p_.getProperty("IO.MQTT_SUBSCRIBE.APOLLO_PASSWORD");
                    apolloURL = p_.getProperty("IO.MQTT_SUBSCRIBE.APOLLO_URL");
                    topic = p_.getProperty("IO.MQTT_SUBSCRIBE.TOPIC_NAME");
                    doneSetup = true;
                }
                apolloURL = p_.getProperty("IO.MQTT_SUBSCRIBE.APOLLO_URL");
                apolloClient = p_.getProperty("IO.MQTT_SUBSCRIBE.APOLLO_CLIENT") + counter;
                counter++;
                mqttClient = new MqttClient(apolloURL, apolloClient);
                MqttConnectOptions connOpts = new MqttConnectOptions();
                connOpts.setCleanSession(true);
                connOpts.setUserName(apolloUserName);
                connOpts.setPassword(apolloPassword.toCharArray());
                mqttClient.connect(connOpts);
                mqttClient.setCallback(this);
                mqttClient.subscribe(topic); // added later
                this.incoming = new LinkedBlockingQueue<>();
                incoming.put("test-12");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Float doTaskLogic(Map map) {
        String m = (String) map.get(AbstractTask.DEFAULT_KEY);

        String pollString = incoming.poll();
        if (pollString != null) {
            if (l.isInfoEnabled()) l.info("TEST:pollString-{}", pollString);
            setLastResult(pollString);
            return Float.valueOf(pollString.length());
        }

        if (pollString == null) {
            setLastResult(null);
        }

        return null;
    }

    @Override
    public void connectionLost(Throwable arg0) {}

    @Override
    public void deliveryComplete(IMqttDeliveryToken arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
        if (l.isInfoEnabled()) l.info("In messageArrived {}", arg1.toString());

        if (arg1 != null) incoming.put(arg1.toString());
        //			setLastResult(arg1.toString());
    }

    @Override
    public float tearDown() {
        try {
            if (mqttClient != null && mqttClient.isConnected()) {
                mqttClient.disconnect();
            }
        } catch (MqttException e) {
            e.printStackTrace();
        }

        return Float.parseFloat("2");
    }

    // Other class members and methods...
}

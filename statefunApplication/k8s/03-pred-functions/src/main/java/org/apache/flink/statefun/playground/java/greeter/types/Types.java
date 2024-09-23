package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.playground.java.greeter.types.generated.UserProfile;
import org.apache.flink.statefun.playground.java.greeter.types.generated.AverageEntry;
import org.apache.flink.statefun.playground.java.greeter.types.BlobReadEntry;
import org.apache.flink.statefun.playground.java.greeter.types.generated.DecisionTreeEntry;
import org.apache.flink.statefun.playground.java.greeter.types.generated.ErrorEstimateEntry;
import org.apache.flink.statefun.playground.java.greeter.types.generated.LinearRegressionEntry;
import org.apache.flink.statefun.playground.java.greeter.types.generated.MqttPublishEntry;
import org.apache.flink.statefun.playground.java.greeter.types.generated.MqttSubscribeEntry;
import org.apache.flink.statefun.playground.java.greeter.types.generated.SenMlEntry;
import org.apache.flink.statefun.playground.java.greeter.types.generated.SourceEntry;

import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;



public final class Types {

    private static final ObjectMapper JSON_OBJ_MAPPER = new ObjectMapper();
    public static final Type<EgressRecord> EGRESS_RECORD_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf("io.statefun.playground", "EgressRecord"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, EgressRecord.class));
    private static final String TYPES_NAMESPACE = "greeter.types";

    public static final Type<UserLogin> USER_LOGIN_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "UserLogin"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, UserLogin.class));
    /*
    public static final Type<AverageEntry> AVERAGE_ENTRY_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "AverageEntry"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, AverageEntry.class));

    public static final Type<BlobReadEntry> BLOB_READ_ENTRY_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "BlobReadEntry"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, BlobReadEntry.class));
    public static final Type<DecisionTreeEntry> DECISION_TREE_ENTRY_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "DecisionTreeEntry"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, DecisionTreeEntry.class));

    public static final Type<ErrorEstimateEntry> ERROR_ESTIMATE_ENTRY_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "ErrorEstimateEntry"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, ErrorEstimateEntry.class));

    public static final Type<LinearRegressionEntry> LINEAR_REGRESSION_ENTRY_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "LinearRegressionEntry"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, LinearRegressionEntry.class));

    public static final Type<MqttPublishEntry> MQTT_PUBLISH_ENTRY_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "MqttPublishEntry"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, MqttPublishEntry.class));

    public static final Type<MqttSubscribeEntry> MQTT_SUBSCRIBE_ENTRY_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "MqttSubscribeEntry"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, MqttSubscribeEntry.class));

    public static final Type<SourceEntry> SOURCE_ENTRY_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "SourceEntry"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, SourceEntry.class));

    public static final Type<SenMlEntry> SENML_ENTRY_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "SenMlEntry"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, SenMlEntry.class));

*/

  public static final Type<BlobReadEntry> BLOB_READ_ENTRY_JSON_TYPE =
          SimpleType.simpleImmutableTypeFrom(
                  TypeName.typeNameOf(TYPES_NAMESPACE, "BlobReadEntry"),
                  JSON_OBJ_MAPPER::writeValueAsBytes,
                  bytes -> JSON_OBJ_MAPPER.readValue(bytes, BlobReadEntry.class));

  public static final Type<UserProfile> USER_PROFILE_PROTOBUF_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameOf(TYPES_NAMESPACE, UserProfile.getDescriptor().getFullName()),
          UserProfile::toByteArray,
          UserProfile::parseFrom);


    // AverageEntry
    public static final Type<AverageEntry> AVERAGE_ENTRY_PROTOBUF_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, AverageEntry.getDescriptor().getFullName()),
                    AverageEntry::toByteArray,
                    AverageEntry::parseFrom);

    // BlobReadEntry
  /*
    public static final Type<BlobReadEntry> BLOB_READ_ENTRY_PROTOBUF_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, BlobReadEntry.getDescriptor().getFullName()),
                    BlobReadEntry::toByteArray,
                    BlobReadEntry::parseFrom);

   */

    // DecisionTreeEntry
    public static final Type<DecisionTreeEntry> DECISION_TREE_ENTRY_PROTOBUF_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, DecisionTreeEntry.getDescriptor().getFullName()),
                    DecisionTreeEntry::toByteArray,
                    DecisionTreeEntry::parseFrom);

    // ErrorEstimateEntry
    public static final Type<ErrorEstimateEntry> ERROR_ESTIMATE_ENTRY_PROTOBUF_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, ErrorEstimateEntry.getDescriptor().getFullName()),
                    ErrorEstimateEntry::toByteArray,
                    ErrorEstimateEntry::parseFrom);

    // LinearRegressionEntry
    public static final Type<LinearRegressionEntry> LINEAR_REGRESSION_ENTRY_PROTOBUF_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, LinearRegressionEntry.getDescriptor().getFullName()),
                    LinearRegressionEntry::toByteArray,
                    LinearRegressionEntry::parseFrom);

    // MqttPublishEntry
    public static final Type<MqttPublishEntry> MQTT_PUBLISH_ENTRY_PROTOBUF_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, MqttPublishEntry.getDescriptor().getFullName()),
                    MqttPublishEntry::toByteArray,
                    MqttPublishEntry::parseFrom);

    // MqttSubscribeEntry
    public static final Type<MqttSubscribeEntry> MQTT_SUBSCRIBE_ENTRY_PROTOBUF_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, MqttSubscribeEntry.getDescriptor().getFullName()),
                    MqttSubscribeEntry::toByteArray,
                    MqttSubscribeEntry::parseFrom);

    // SenMlEntry
    public static final Type<SenMlEntry> SEN_ML_ENTRY_PROTOBUF_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, SenMlEntry.getDescriptor().getFullName()),
                    SenMlEntry::toByteArray,
                    SenMlEntry::parseFrom);

    // SourceEntry
    public static final Type<SourceEntry> SOURCE_ENTRY_PROTOBUF_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, SourceEntry.getDescriptor().getFullName()),
                    SourceEntry::toByteArray,
                    SourceEntry::parseFrom);


    private Types() {
    }
}

package org.apache.flink.statefun.playground.java.greeter.types;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

public final class Types {

    private Types() {
    }

    private static final ObjectMapper JSON_OBJ_MAPPER = new ObjectMapper();
    private static final String TYPES_NAMESPACE = "greeter.types";

    public static final Type<UserLogin> USER_LOGIN_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "UserLogin"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, UserLogin.class));

    public static final Type<AnnotateEntry> ANNOTATE_ENTRY_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "AnnotateEntry"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, AnnotateEntry.class));

    public static final Type<BlobUploadEntry> BlobUpload_ENTRY_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "BlobUploadEntry"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, BlobUploadEntry.class));

    public static final Type<DbEntry> Db_ENTRY_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "DbEntry"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, DbEntry.class));

    public static final Type<MqttPublishEntry> MqttPublish_ENTRY_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "MqttPublishEntry"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, MqttPublishEntry.class));
    public static final Type<TrainEntry> Train_ENTRY_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "TrainEntry"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, TrainEntry.class));

    public static final Type<SourceEntry> Source_ENTRY_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "SourceEntry"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, SourceEntry.class));



    


  /*
  public static final Type<UserProfile> USER_PROFILE_PROTOBUF_TYPE =
      SimpleType.simpleImmutableTypeFrom(
          TypeName.typeNameOf(TYPES_NAMESPACE, UserProfile.getDescriptor().getFullName()),
          UserProfile::toByteArray,
          UserProfile::parseFrom);

   */

    public static final Type<EgressRecord> EGRESS_RECORD_JSON_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf("io.statefun.playground", "EgressRecord"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, EgressRecord.class));
}

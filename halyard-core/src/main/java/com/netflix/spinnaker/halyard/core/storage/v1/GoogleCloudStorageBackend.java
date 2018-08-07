/*
 * Copyright 2018 Google, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.spinnaker.halyard.core.storage.v1;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;
import com.netflix.spinnaker.halyard.core.provider.v1.google.GoogleCredentials;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class GoogleCloudStorageBackend implements StorageBackend {
  private Storage storage;
  private String bucket;

  public GoogleCloudStorageBackend(String bucket) throws IOException {
    this(bucket, null);
  }

  public GoogleCloudStorageBackend(String bucket, String credentialJsonPath) throws IOException {
    HttpTransport httpTransport = GoogleCredentials.buildHttpTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    GoogleCredential credential = loadCredentials(httpTransport, jsonFactory, credentialJsonPath);
    this.storage = new Storage.Builder(httpTransport, jsonFactory, GoogleCredentials.setHttpTimeout(credential))
        .setApplicationName("halyard")
        .build();
    this.bucket = bucket;
  }

  private GoogleCredential loadCredentials(HttpTransport transport, JsonFactory factory, String jsonPath) throws IOException {
    GoogleCredential credential;
    if (!StringUtils.isEmpty(jsonPath)) {
      FileInputStream stream = new FileInputStream(jsonPath);
      credential = GoogleCredential.fromStream(stream, transport, factory)
          .createScoped(Collections.singleton(StorageScopes.DEVSTORAGE_FULL_CONTROL));
      log.info("Loaded credentials from " + jsonPath);
    } else {
      log.info("Using default application credentials.");
      credential = GoogleCredential.getApplicationDefault();
    }
    return credential;
  }

  private void writeObject(String name, String type, byte[] bytes) throws IOException {
    ByteArrayContent content = new ByteArrayContent(type, bytes);
    StorageObject object = new StorageObject()
        .setBucket(bucket)
        .setName(name);
    storage.objects()
        .insert(bucket, object, content)
        .execute();
  }

  public void writeTextObject(String name, String contents) throws IOException {
    writeObject(name, "application/text", contents.getBytes());
  }

  public void writeTextObject(Path path, String contents) throws IOException {
    writeTextObject(path.toFile().getName(), contents);
  }

  public InputStream getObjectStream(String name) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    storage.objects().get(bucket, name).executeMediaAndDownloadTo(output);
    return new ByteArrayInputStream(output.toByteArray());
  }
}
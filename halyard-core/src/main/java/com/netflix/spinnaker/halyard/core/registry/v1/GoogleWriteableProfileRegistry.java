/*
 * Copyright 2017 Google, Inc.
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

package com.netflix.spinnaker.halyard.core.registry.v1;

import com.netflix.spinnaker.halyard.core.error.v1.HalException;
import com.netflix.spinnaker.halyard.core.problem.v1.Problem.Severity;
import com.netflix.spinnaker.halyard.core.problem.v1.ProblemBuilder;
import com.netflix.spinnaker.halyard.core.storage.v1.GoogleCloudStorageBackend;
import com.netflix.spinnaker.halyard.core.storage.v1.StorageBackend;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

@Slf4j
public class GoogleWriteableProfileRegistry {
  private StorageBackend storageBackend;

  @Autowired
  String spinconfigBucket;

  @Autowired
  GoogleProfileReader googleProfileReader;

  GoogleWriteableProfileRegistry(WriteableProfileRegistryProperties properties) {
    try {
      this.storageBackend = new GoogleCloudStorageBackend(spinconfigBucket, properties.getJsonPath());
    } catch (IOException e) {
      throw new RuntimeException("Failed to load json credential", e);
    }
  }

  public void writeBom(String version, String contents) {
    String name = googleProfileReader.bomPath(version);
    writeTextObject(name, contents);
  }

  public void writeArtifactConfig(BillOfMaterials bom, String artifactName, String profileName, String contents) {
    String version = bom.getArtifactVersion(artifactName);
    String name = googleProfileReader.profilePath(artifactName, version, profileName);

    writeTextObject(name, contents);
  }

  public void writeVersions(String versions) {
    writeTextObject("versions.yml", versions);
  }

  private void writeTextObject(String name, String contents) {
    try {
      storageBackend.writeTextObject(name, contents);
    } catch (IOException e) {
      log.error("Failed to write new object " + name, e);
      throw new HalException(new ProblemBuilder(Severity.FATAL, "Failed to write to " + name + ": " + e.getMessage()).build());
    }
  }
}

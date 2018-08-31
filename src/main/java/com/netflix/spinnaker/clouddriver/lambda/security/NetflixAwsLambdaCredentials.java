/*
 * Copyright 2018 Netflix, Inc.
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
 */

package com.netflix.spinnaker.clouddriver.lambda.security;

import com.netflix.spinnaker.clouddriver.aws.security.NetflixAmazonCredentials;

public class NetflixAwsLambdaCredentials extends NetflixAmazonCredentials {
  private static final String CLOUD_PROVIDER = "awslambda";

  public NetflixAwsLambdaCredentials(NetflixAmazonCredentials copy) {
    super(copy, copy.getCredentialsProvider());
  }

  @Override
  public String getCloudProvider() {
    return CLOUD_PROVIDER;
  }

  @Override
  public String getAccountType() {
    return CLOUD_PROVIDER;
  }
}

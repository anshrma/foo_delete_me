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

package com.netflix.spinnaker.clouddriver.lambda.provider.agent;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.ListFunctionsRequest;
import com.amazonaws.services.lambda.model.ListFunctionsResult;
import com.amazonaws.services.lambda.model.FunctionConfiguration;
import com.netflix.spinnaker.cats.agent.AgentDataType;
import com.netflix.spinnaker.cats.cache.CacheData;
import com.netflix.spinnaker.cats.cache.DefaultCacheData;
import com.netflix.spinnaker.cats.provider.ProviderCache;
import com.netflix.spinnaker.clouddriver.aws.security.AmazonClientProvider;
import com.netflix.spinnaker.clouddriver.aws.security.NetflixAmazonCredentials;
import com.netflix.spinnaker.clouddriver.lambda.cache.Keys;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.netflix.spinnaker.cats.agent.AgentDataType.Authority.AUTHORITATIVE;
import static com.netflix.spinnaker.clouddriver.lambda.cache.Keys.Namespace.LAMBDA_NAME;

public class AwsLambdaFunctionCachingAgent extends AwsAbstractLambdaCachingAgent<String>{

  private static final Collection<AgentDataType> types = Collections.unmodifiableCollection(Arrays.asList(
    AUTHORITATIVE.forType(LAMBDA_NAME.toString())
  ));
  private final Logger log = LoggerFactory.getLogger(getClass());

  public AwsLambdaFunctionCachingAgent(NetflixAmazonCredentials account, String region, AmazonClientProvider amazonClientProvider, AWSCredentialsProvider awsCredentialsProvider) {
    super(account, region, amazonClientProvider, awsCredentialsProvider);
  }

  @Override
  public String getAgentType() {
    return accountName + "/" + region + "/" + getClass().getSimpleName();
  }

  @Override
  public Collection<AgentDataType> getProvidedDataTypes() {
    return types;
  }

  @Override
  protected List<String> getItems(AWSLambda lambda, ProviderCache providerCache) {
    List<FunctionConfiguration> lstFunction = new ArrayList<>();
    LinkedList allLambdaFunctionArns = new LinkedList<>();
    String nextMarker = null;

    ListAllAwsLambdaFunctions(lambda, lstFunction, nextMarker);

    for (FunctionConfiguration x:lstFunction){
      allLambdaFunctionArns.add(x.getFunctionArn());
    }

    return allLambdaFunctionArns;

  }

  static void ListAllAwsLambdaFunctions(AWSLambda lambda, List<FunctionConfiguration> lstFunction, String nextMarker) {
    do {
      ListFunctionsRequest listFunctionsRequest = new ListFunctionsRequest();
      if (nextMarker != null) {
        listFunctionsRequest.setMarker(nextMarker);
      }

      ListFunctionsResult listFunctionsResult  = lambda.listFunctions(listFunctionsRequest);
      lstFunction.addAll(listFunctionsResult.getFunctions());
      nextMarker = listFunctionsResult.getNextMarker();

    } while (nextMarker != null && nextMarker.length() != 0);
  }

  @Override
  protected Map<String, Collection<CacheData>> generateFreshData(Collection<String> lambdaFunctionArns) {
    Collection<CacheData> dataPoints = new LinkedList<>();
    for (String lambdaFunctionArn : lambdaFunctionArns) {
      String functionName = StringUtils.substringAfterLast(lambdaFunctionArn, "/");
      Map<String, Object> attributes = convertLambdaFunctionArnToAttributes(accountName, region, lambdaFunctionArn);

      String key = Keys.getLambdaFunctionKey(accountName, region, functionName);
      dataPoints.add(new DefaultCacheData(key, attributes, Collections.emptyMap()));
    }

    log.info("Caching " + dataPoints.size() + " ECS clusters in " + getAgentType());
    Map<String, Collection<CacheData>> dataMap = new HashMap<>();
    dataMap.put(LAMBDA_NAME.toString(), dataPoints);

    return dataMap;
  }
  public static  Map<String, Object> convertLambdaFunctionArnToAttributes(String accountName, String region, String lambdaFunctionArn){
    String lambdaFunctionName = StringUtils.substringAfterLast(lambdaFunctionArn, "/");

    Map<String, Object> attributes = new HashMap<>();
    attributes.put("account", accountName);
    attributes.put("region", region);
    attributes.put("lambdaFunctionName", lambdaFunctionName);
    attributes.put("lambdaFunctionArn", lambdaFunctionArn);

    return attributes;
  }

}

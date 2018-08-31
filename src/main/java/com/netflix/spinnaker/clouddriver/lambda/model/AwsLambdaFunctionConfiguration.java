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

package com.netflix.spinnaker.clouddriver.lambda.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import com.amazonaws.services.lambda.model.FunctionConfiguration;

@Data
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class AwsLambdaFunctionConfiguration<FunctionConfiguration> {

String Name;
String Arn;
String CodeSize;

public AwsLambdaFunctionConfiguration (String name,String arn,String codesize){
  this.Arn = arn;
  this.Name = name;
  this.CodeSize = codesize;

}
}

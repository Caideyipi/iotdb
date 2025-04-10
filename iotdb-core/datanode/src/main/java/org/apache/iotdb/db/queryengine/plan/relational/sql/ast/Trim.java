/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Trim extends Expression {
  private final Specification specification;
  private final Expression trimSource;
  @Nullable private final Expression trimCharacter;

  public Trim(Specification specification, Expression trimSource) {
    super(null);
    this.specification = requireNonNull(specification, "specification is null");
    this.trimSource = requireNonNull(trimSource, "trimSource is null");
    this.trimCharacter = null;
  }

  public Trim(Specification specification, Expression trimSource, Expression trimCharacter) {
    super(null);
    this.specification = requireNonNull(specification, "specification is null");
    this.trimSource = requireNonNull(trimSource, "trimSource is null");
    this.trimCharacter = trimCharacter;
  }

  public Trim(NodeLocation location, Specification specification, Expression trimSource) {
    super(requireNonNull(location, "location is null"));
    this.specification = requireNonNull(specification, "specification is null");
    this.trimSource = requireNonNull(trimSource, "trimSource is null");
    this.trimCharacter = null;
  }

  public Trim(
      NodeLocation location,
      Specification specification,
      Expression trimSource,
      Expression trimCharacter) {
    super(requireNonNull(location, "location is null"));
    this.specification = requireNonNull(specification, "specification is null");
    this.trimSource = requireNonNull(trimSource, "trimSource is null");
    this.trimCharacter = requireNonNull(trimCharacter, "trimCharacter is null");
  }

  public Specification getSpecification() {
    return specification;
  }

  public Expression getTrimSource() {
    return trimSource;
  }

  public Optional<Expression> getTrimCharacter() {
    return Optional.ofNullable(trimCharacter);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitTrim(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    nodes.add(trimSource);
    if (trimCharacter != null) {
      nodes.add(trimCharacter);
    }
    return nodes.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Trim that = (Trim) o;
    return specification == that.specification
        && Objects.equals(trimSource, that.trimSource)
        && Objects.equals(trimCharacter, that.trimCharacter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(specification, trimSource, trimCharacter);
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    Trim otherTrim = (Trim) other;
    return specification == otherTrim.specification;
  }

  public enum Specification {
    BOTH("trim"),
    LEADING("ltrim"),
    TRAILING("rtrim");

    private final String functionName;

    Specification(String functionName) {
      this.functionName = requireNonNull(functionName, "functionName is null");
    }

    public String getFunctionName() {
      return functionName;
    }
  }
}

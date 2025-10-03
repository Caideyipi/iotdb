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

package com.timecho.iotdb.auth;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckContext;
import org.apache.iotdb.db.queryengine.plan.relational.security.TreeAccessCheckVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.LoadConfigurationStatement;
import org.apache.iotdb.rpc.RpcUtils;

import java.util.function.Supplier;

public class StrictTreeAccessCheckVisitor extends TreeAccessCheckVisitor {

  @Override
  protected boolean checkHasGlobalAuth(
      IAuditEntity context, PrivilegeType requiredPrivilege, Supplier<String> auditObject) {
    // only check SYSTEM, SECURITY, AUDIT
    return super.checkHasGlobalAuth(
        context, requiredPrivilege.getReplacedPrivilegeType(), auditObject);
  }

  @Override
  public TSStatus visitAuthor(AuthorStatement statement, TreeAccessCheckContext context) {
    AuthorType authorType = statement.getAuthorType();
    Supplier<String> auditObject;
    User queriedUser = null;
    switch (authorType) {
      case CREATE_USER:
      case DROP_USER:
        context
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY);
        return checkGlobalAuth(
            context.setAuditLogOperation(AuditLogOperation.DDL),
            PrivilegeType.SECURITY,
            statement::getUserName);

      case UPDATE_USER:
      case RENAME_USER:
        context
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY);
        if (context.getUsername().equals(statement.getUserName())) {
          return RpcUtils.SUCCESS_STATUS;
        }
        return checkGlobalAuth(
            context.setAuditLogOperation(AuditLogOperation.DDL),
            PrivilegeType.SECURITY,
            statement::getUserName);

      case LIST_USER:
        context
            .setAuditLogOperation(AuditLogOperation.QUERY)
            .setPrivilegeType(PrivilegeType.SECURITY);
        return RpcUtils.SUCCESS_STATUS;

      case LIST_USER_PRIVILEGE:
        context
            .setAuditLogOperation(AuditLogOperation.QUERY)
            .setPrivilegeType(PrivilegeType.SECURITY);
        if (context.getUsername().equals(statement.getUserName())) {
          return RpcUtils.SUCCESS_STATUS;
        }
        queriedUser = AuthorityChecker.getUser(statement.getUserName());
        if (queriedUser != null
            && User.INTERNAL_SYSTEM_ADMIN == context.getUserId()
            && queriedUser.checkSysPrivilege(PrivilegeType.SYSTEM)) {
          return RpcUtils.SUCCESS_STATUS;
        }
        if (queriedUser != null
            && User.INTERNAL_AUDIT_ADMIN == context.getUserId()
            && queriedUser.checkSysPrivilege(PrivilegeType.AUDIT)) {
          return RpcUtils.SUCCESS_STATUS;
        }
        return checkGlobalAuth(
            context.setAuditLogOperation(AuditLogOperation.QUERY),
            PrivilegeType.SECURITY,
            statement::getUserName);

      case LIST_ROLE_PRIVILEGE:
        context
            .setAuditLogOperation(AuditLogOperation.QUERY)
            .setPrivilegeType(PrivilegeType.SECURITY);
        if (AuthorityChecker.checkRole(context.getUsername(), statement.getRoleName())
            || User.INTERNAL_SYSTEM_ADMIN == context.getUserId()
            || User.INTERNAL_AUDIT_ADMIN == context.getUserId()) {
          return RpcUtils.SUCCESS_STATUS;
        }
        return checkGlobalAuth(
            context.setAuditLogOperation(AuditLogOperation.QUERY),
            PrivilegeType.SECURITY,
            statement::getRoleName);

      case LIST_ROLE:
        context
            .setAuditLogOperation(AuditLogOperation.QUERY)
            .setPrivilegeType(PrivilegeType.SECURITY);
        // LIST ROLE
        if (statement.getUserName() == null) {
          if (User.INTERNAL_SYSTEM_ADMIN != context.getUserId()
              && User.INTERNAL_AUDIT_ADMIN != context.getUserId()
              // getRoleName will return null
              && !checkHasGlobalAuth(
                  context.setAuditLogOperation(AuditLogOperation.QUERY),
                  PrivilegeType.SECURITY,
                  statement::getRoleName)) {
            // convert to list role of current user
            statement.setUserName(context.getUsername());
          }
          return RpcUtils.SUCCESS_STATUS;
        }
        // LIST ROLE OF USER
        if (context.getUsername().equals(statement.getUserName())) {
          return RpcUtils.SUCCESS_STATUS;
        }
        queriedUser = AuthorityChecker.getUser(statement.getUserName());
        if (queriedUser != null
            && User.INTERNAL_SYSTEM_ADMIN == context.getUserId()
            && queriedUser.checkSysPrivilege(PrivilegeType.SYSTEM)) {
          return RpcUtils.SUCCESS_STATUS;
        }
        if (queriedUser != null
            && User.INTERNAL_AUDIT_ADMIN == context.getUserId()
            && queriedUser.checkSysPrivilege(PrivilegeType.AUDIT)) {
          return RpcUtils.SUCCESS_STATUS;
        }
        return checkGlobalAuth(
            context.setAuditLogOperation(AuditLogOperation.QUERY),
            PrivilegeType.SECURITY,
            statement::getUserName);

      case CREATE_ROLE:
      case DROP_ROLE:
      case GRANT_USER_ROLE:
      case REVOKE_USER_ROLE:
        context
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY);
        return checkGlobalAuth(
            context.setAuditLogOperation(AuditLogOperation.QUERY),
            PrivilegeType.SECURITY,
            statement::getUserName);

      case REVOKE_USER:
      case GRANT_USER:
      case GRANT_ROLE:
      case REVOKE_ROLE:
        context
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY);
        auditObject =
            () ->
                authorType == AuthorType.REVOKE_USER || authorType == AuthorType.GRANT_USER
                    ? statement.getUserName()
                    : statement.getRoleName();
        boolean isGrantRole = authorType == AuthorType.GRANT_ROLE;
        for (String s : statement.getPrivilegeList()) {
          PrivilegeType privilegeType = PrivilegeType.valueOf(s.toUpperCase());
          if (privilegeType.isSystemPrivilege()) {
            if (isGrantRole) {
              return AuthorityChecker.getTSStatus(
                  false,
                  "Admin privileges can not be granted to roles when separation of admin power is enabled.");
            }
            if (authorType == AuthorType.REVOKE_USER) {
              if (context.getUserId() == User.INTERNAL_SECURITY_ADMIN
                  && context.getUsername().equals(statement.getUserName())
                  && privilegeType == PrivilegeType.SECURITY) {
                return AuthorityChecker.getTSStatus(
                    false, "Can not revoke SECURITY power from builtin security admin");
              }
              if (context.getUserId() == User.INTERNAL_SYSTEM_ADMIN
                  && context.getUsername().equals(statement.getUserName())
                  && privilegeType == PrivilegeType.SYSTEM) {
                return AuthorityChecker.getTSStatus(
                    false, "Can not revoke SYSTEM power from builtin system admin");
              }
              if (context.getUserId() == User.INTERNAL_AUDIT_ADMIN
                  && context.getUsername().equals(statement.getUserName())
                  && privilegeType == PrivilegeType.AUDIT) {
                return AuthorityChecker.getTSStatus(
                    false, "Can not revoke AUDIT power from builtin audit admin");
              }
            }
            if (statement.getGrantOpt()) {
              return AuthorityChecker.getTSStatus(
                  false,
                  "Admin privileges do not support grant options when separation of admin power is enable.");
            }
            if (!AuthorityChecker.checkSystemPermissionGrantOption(
                context.getUsername(), privilegeType)) {
              return AuthorityChecker.getTSStatus(
                  false,
                  "Has no permission to execute "
                      + authorType
                      + ", please ensure you have these privileges and the grant option is TRUE when granted.");
            }
          } else if (privilegeType.isPathPrivilege()) {
            if (authorType == AuthorType.REVOKE_USER) {
              return checkGlobalAuth(
                  context.setAuditLogOperation(AuditLogOperation.DDL),
                  PrivilegeType.SECURITY,
                  auditObject);
            }
            if (!checkHasGlobalAuth(
                    context.setAuditLogOperation(AuditLogOperation.DDL),
                    PrivilegeType.SECURITY,
                    auditObject)
                && !AuthorityChecker.checkPathPermissionGrantOption(
                    context.getUsername(), privilegeType, statement.getNodeNameList())) {
              return AuthorityChecker.getTSStatus(
                  false,
                  "Has no permission to execute "
                      + authorType
                      + ", please ensure you have these privileges and the grant option is TRUE when granted.");
            }
          } else {
            return AuthorityChecker.getTSStatus(
                false, "Not support Relation statement in tree sql_dialect");
          }
        }
        return RpcUtils.SUCCESS_STATUS;
      default:
        throw new UnsupportedOperationException("Unsupported authorType: " + authorType);
    }
  }

  @Override
  public TSStatus visitLoadConfiguration(
      LoadConfigurationStatement loadConfigurationStatement, TreeAccessCheckContext context) {
    return AuthorityChecker.getTSStatus(
        false, "This operation is forbidden while enabling separation of powers.");
  }
}

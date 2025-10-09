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
import org.apache.iotdb.commons.exception.auth.AccessDeniedException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControlImpl;
import org.apache.iotdb.db.queryengine.plan.relational.security.ITableAuthChecker;
import org.apache.iotdb.db.queryengine.plan.relational.security.ITableAuthCheckerImpl;
import org.apache.iotdb.db.queryengine.plan.relational.security.TableModelPrivilege;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

public class StrictAccessControlImpl extends AccessControlImpl {
  public StrictAccessControlImpl(
      ITableAuthChecker authChecker, StrictTreeAccessCheckVisitor visitor) {
    super(authChecker, visitor);
  }

  @Override
  public void checkUserCanRunRelationalAuthorStatement(
      String userName, RelationalAuthorStatement statement, IAuditEntity auditEntity) {
    AuthorRType type = statement.getAuthorType();
    User queriedUser = null;
    switch (type) {
      case CREATE_USER:
      case DROP_USER:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY);
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.SECURITY, auditEntity);
        return;
      case UPDATE_USER:
      case RENAME_USER:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY);
        if (auditEntity.getUsername().equals(statement.getUserName())) {
          ITableAuthCheckerImpl.recordAuditLog(auditEntity.setResult(true), statement::getUserName);
          return;
        }
        authChecker.checkGlobalPrivilege(userName, TableModelPrivilege.SECURITY, auditEntity);
        return;
      case LIST_USER_PRIV:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.QUERY)
            .setPrivilegeType(PrivilegeType.SECURITY);
        if (auditEntity.getUsername().equals(statement.getUserName())) {
          ITableAuthCheckerImpl.recordAuditLog(auditEntity.setResult(true), statement::getUserName);
          return;
        }
        queriedUser = AuthorityChecker.getUser(statement.getUserName());
        if (queriedUser != null
            && User.INTERNAL_SYSTEM_ADMIN == auditEntity.getUserId()
            && queriedUser.checkSysPrivilege(PrivilegeType.SYSTEM)) {
          ITableAuthCheckerImpl.recordAuditLog(auditEntity.setResult(true), statement::getUserName);
          return;
        }
        if (queriedUser != null
            && User.INTERNAL_AUDIT_ADMIN == auditEntity.getUserId()
            && queriedUser.checkSysPrivilege(PrivilegeType.AUDIT)) {
          ITableAuthCheckerImpl.recordAuditLog(auditEntity.setResult(true), statement::getUserName);
          return;
        }
        authChecker.checkGlobalPrivilege(
            auditEntity.getUsername(), TableModelPrivilege.SECURITY, auditEntity);
        return;
      case LIST_USER:
        auditEntity.setAuditLogOperation(AuditLogOperation.QUERY);
        ITableAuthCheckerImpl.recordAuditLog(auditEntity.setResult(true), statement::getUserName);
        return;

      case CREATE_ROLE:
      case DROP_ROLE:
      case GRANT_USER_ROLE:
      case REVOKE_USER_ROLE:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY);
        authChecker.checkGlobalPrivilege(
            auditEntity.getUsername(), TableModelPrivilege.SECURITY, auditEntity);
        return;

      case LIST_ROLE:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.QUERY)
            .setPrivilegeType(PrivilegeType.SECURITY);
        // LIST ROLE
        if (statement.getUserName() == null) {
          if (User.INTERNAL_SYSTEM_ADMIN != auditEntity.getUserId()
              && User.INTERNAL_AUDIT_ADMIN != auditEntity.getUserId()
              && !hasGlobalPrivilege(auditEntity, PrivilegeType.SECURITY)) {
            // convert to list role of current user
            statement.setUserName(auditEntity.getUsername());
          }
          ITableAuthCheckerImpl.recordAuditLog(auditEntity.setResult(true), statement::getRoleName);
          return;
        }
        // LIST ROLE OF USER
        if (auditEntity.getUsername().equals(statement.getUserName())) {
          ITableAuthCheckerImpl.recordAuditLog(auditEntity.setResult(true), statement::getUserName);
          return;
        }
        queriedUser = AuthorityChecker.getUser(statement.getUserName());
        if (queriedUser != null
            && User.INTERNAL_SYSTEM_ADMIN == auditEntity.getUserId()
            && queriedUser.checkSysPrivilege(PrivilegeType.SYSTEM)) {
          ITableAuthCheckerImpl.recordAuditLog(auditEntity.setResult(true), statement::getUserName);
          return;
        }
        if (queriedUser != null
            && User.INTERNAL_AUDIT_ADMIN == auditEntity.getUserId()
            && queriedUser.checkSysPrivilege(PrivilegeType.AUDIT)) {
          ITableAuthCheckerImpl.recordAuditLog(auditEntity.setResult(true), statement::getUserName);
          return;
        }
        authChecker.checkGlobalPrivilege(
            auditEntity.getUsername(), TableModelPrivilege.SECURITY, auditEntity);
        return;

      case LIST_ROLE_PRIV:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.QUERY)
            .setPrivilegeType(PrivilegeType.SECURITY);
        if (AuthorityChecker.checkRole(auditEntity.getUsername(), statement.getRoleName())
            || User.INTERNAL_SYSTEM_ADMIN == auditEntity.getUserId()
            || User.INTERNAL_AUDIT_ADMIN == auditEntity.getUserId()) {
          ITableAuthCheckerImpl.recordAuditLog(auditEntity.setResult(true), statement::getRoleName);
          return;
        }
        authChecker.checkGlobalPrivilege(
            auditEntity.getUsername(), TableModelPrivilege.SECURITY, auditEntity);
        return;

      case GRANT_ROLE_ANY:
      case GRANT_USER_ANY:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY)
            .setDatabase(statement.getDatabase());
        if (hasGlobalPrivilege(auditEntity, PrivilegeType.SECURITY)) {
          ITableAuthCheckerImpl.recordAuditLog(
              auditEntity.setResult(true), () -> statement.getUserName() + statement.getRoleName());
          return;
        }
        for (PrivilegeType privilegeType : statement.getPrivilegeTypes()) {
          authChecker.checkAnyScopePrivilegeGrantOption(
              userName, TableModelPrivilege.getTableModelType(privilegeType), auditEntity);
        }
        return;
      case REVOKE_ROLE_ANY:
      case REVOKE_USER_ANY:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY)
            .setDatabase(statement.getDatabase());
        authChecker.checkGlobalPrivilege(
            auditEntity.getUsername(), TableModelPrivilege.SECURITY, auditEntity);
        return;

      case GRANT_ROLE_ALL:
      case REVOKE_ROLE_ALL:
      case GRANT_USER_ALL:
      case REVOKE_USER_ALL:
        throw new AccessDeniedException(
            "Grant/Revoke ALL is not allowed when separation of admin powers is enabled");

      case GRANT_USER_DB:
      case GRANT_ROLE_DB:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY)
            .setDatabase(statement.getDatabase());
        if (hasGlobalPrivilege(auditEntity, PrivilegeType.SECURITY)) {
          ITableAuthCheckerImpl.recordAuditLog(
              auditEntity.setResult(true), () -> statement.getUserName() + statement.getRoleName());
          return;
        }
        for (PrivilegeType privilegeType : statement.getPrivilegeTypes()) {
          authChecker.checkDatabasePrivilegeGrantOption(
              userName,
              statement.getDatabase(),
              TableModelPrivilege.getTableModelType(privilegeType),
              auditEntity);
        }
        return;

      case REVOKE_USER_DB:
      case REVOKE_ROLE_DB:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY)
            .setDatabase(statement.getDatabase());
        authChecker.checkGlobalPrivilege(
            auditEntity.getUsername(), TableModelPrivilege.SECURITY, auditEntity);
        return;

      case GRANT_USER_TB:
      case GRANT_ROLE_TB:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY)
            .setDatabase(statement.getDatabase());
        if (hasGlobalPrivilege(auditEntity, PrivilegeType.SECURITY)) {
          ITableAuthCheckerImpl.recordAuditLog(
              auditEntity.setResult(true), () -> statement.getUserName() + statement.getRoleName());
          return;
        }
        for (PrivilegeType privilegeType : statement.getPrivilegeTypes()) {
          authChecker.checkTablePrivilegeGrantOption(
              userName,
              new QualifiedObjectName(statement.getDatabase(), statement.getTableName()),
              TableModelPrivilege.getTableModelType(privilegeType),
              auditEntity);
        }
        return;
      case REVOKE_USER_TB:
      case REVOKE_ROLE_TB:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY)
            .setDatabase(statement.getDatabase());
        authChecker.checkGlobalPrivilege(
            auditEntity.getUsername(), TableModelPrivilege.SECURITY, auditEntity);
        return;

      case GRANT_ROLE_SYS:
        throw new AccessDeniedException(
            "Grant admin privileges to roles is not allowed when separation of admin powers is enabled");

      case GRANT_USER_SYS:
      case REVOKE_USER_SYS:
      case REVOKE_ROLE_SYS:
        auditEntity
            .setAuditLogOperation(AuditLogOperation.DDL)
            .setPrivilegeType(PrivilegeType.SECURITY);
        for (PrivilegeType privilegeType : statement.getPrivilegeTypes()) {
          if (statement.isGrantOption()) {
            throw new AccessDeniedException(
                "Admin privileges do not support grant options when separation of admin power is enabled.");
          }
          if (type == AuthorRType.REVOKE_USER_SYS) {
            if (auditEntity.getUserId() == User.INTERNAL_SECURITY_ADMIN
                && auditEntity.getUsername().equals(statement.getUserName())
                && privilegeType == PrivilegeType.SECURITY) {
              throw new AccessDeniedException(
                  "Can not revoke SECURITY power from builtin security admin");
            }
            if (auditEntity.getUserId() == User.INTERNAL_SYSTEM_ADMIN
                && auditEntity.getUsername().equals(statement.getUserName())
                && privilegeType == PrivilegeType.SYSTEM) {
              throw new AccessDeniedException(
                  "Can not revoke SYSTEM power from builtin system admin");
            }
            if (auditEntity.getUserId() == User.INTERNAL_AUDIT_ADMIN
                && auditEntity.getUsername().equals(statement.getUserName())
                && privilegeType == PrivilegeType.AUDIT) {
              throw new AccessDeniedException(
                  "Can not revoke AUDIT power from builtin audit admin");
            }
          }
          checkGrantOrRevokeAdminPrivilege(userName, privilegeType, auditEntity);
        }
        break;
    }
  }

  @Override
  public boolean hasGlobalPrivilege(IAuditEntity entity, PrivilegeType privilegeType) {
    return super.hasGlobalPrivilege(entity, privilegeType.getReplacedPrivilegeType());
  }

  @Override
  public void checkUserIsAdmin(IAuditEntity entity) {
    throw new AccessDeniedException(
        "This operation is forbidden while enabling separation of powers.");
  }

  @Override
  public TSStatus allowUserToLogin(String userName) {
    // user should not be null here because the username and password are checked before this
    // judgment
    User user = AuthorityChecker.getUser(userName);
    if (user.getUserId() == AuthorityChecker.SUPER_USER_ID) {
      return RpcUtils.getStatus(
          TSStatusCode.NO_PERMISSION,
          "SUPER USER is not allowed to login when separation of admin powers is enabled.");
    }
    return RpcUtils.SUCCESS_STATUS;
  }

  private void checkGrantOrRevokeAdminPrivilege(
      String userName, PrivilegeType privilege, IAuditEntity auditEntity) {
    TSStatus result =
        AuthorityChecker.getGrantOptTSStatus(
            AuthorityChecker.checkSystemPermissionGrantOption(userName, privilege), privilege);
    if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      ITableAuthCheckerImpl.recordAuditLog(
          auditEntity
              .setAuditLogOperation(privilege.getAuditLogOperation())
              .setPrivilegeType(privilege)
              .setResult(false),
          () -> AuthorityChecker.ANY_SCOPE);
      throw new AccessDeniedException("Only the builtin admin can grant/revoke admin permissions");
    }
    ITableAuthCheckerImpl.recordAuditLog(
        auditEntity
            .setAuditLogOperation(privilege.getAuditLogOperation())
            .setPrivilegeType(privilege)
            .setResult(true),
        () -> AuthorityChecker.ANY_SCOPE);
  }
}

---
title: "Helm Chart"
slug: /chart
keyword: chart
license: "This software is licensed under the Apache License version 2."
---

# Apache Gravitino Helm Chart

This Helm chart deploys Apache Gravitino on Kubernetes with customizable configurations.

## Prerequisites

- Kubernetes 1.29+
- Helm 3+

## Update Chart Dependency

The Gravitino Helm chart has not yet been officially released.   
To proceed, please clone the repository, navigate to the chart directory [charts](../dev/charts), and execute the Helm dependency update command.

```console
helm dependency update [CHART]
```

## View Chart values

You can customize values.yaml parameters to override chart default settings. Additionally, Gravitino configurations in gravitino.conf can be modified through Helm values.yaml.

To display the default values of the Gravitino chart, run:

```console
helm show values [CHART]
```

## Install Helm Chart

```console
helm install [RELEASE_NAME] [CHART] [flags]
```

### Deploy with Default Configuration

Run the following command to deploy Gravitino using the default settings, specify container image versions using --set image.tag=x.y.z (replace x, y, z with the expected version numbers):

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace --set image.tag=<x.y.z>
```

### Deploy with Custom Configuration

To customize the deployment, use the --set flag to override specific values:

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set key1=val1,key2=val2,...
```

Alternatively, you can provide a custom values.yaml file:

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace -f /path/to/chart/resources/scenarios/ci-values.yaml
```

_Note: \
[ci-values.yaml](../dev/charts/gravitino/resources/scenarios/ci-values.yaml) is an example scenario to deploy._

### Deploying Gravitino with MySQL as the Storage Backend

To deploy both Gravitino and MySQL, where MySQL is used as the storage backend, enable the built-in MySQL instance:

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set mysql.enabled=true
```

#### Disable Dynamic Storage Provisioning

By default, the MySQL PersistentVolumeClaim(PVC) storage class is local-path. To disable dynamic provisioning, set the storage class to "-":

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set mysql.enabled=true \
  --set global.defaultStorageClass="-"
```

You must then manually create a PersistentVolume (PV).

### Deploy Gravitino using an existed MySQL Database

Ensure you have the following MySQL credentials ready: Username, Password, Database Name. When creating your database, we recommend calling it `gravitino`.

Before deploying Gravitino, initialize your existing MySQL instance and create the necessary tables required for Gravitino to function properly.

```console
mysql -h database-1.***.***.rds.amazonaws.com -P 3306 -u <YOUR-USERNAME> -p <YOUR-PASSWORD> < schema-0.*.0-mysql.sql
```

Use Helm to install or upgrade Gravitino, specifying the MySQL connection details.

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set entity.jdbcUrl="jdbc:mysql://database-1.***.***.rds.amazonaws.com:3306/gravitino" \
  --set entity.jdbcDriver="com.mysql.cj.jdbc.Driver" \
  --set entity.jdbcUser="admin" \
  --set entity.jdbcPassword="admin123"
```

_Note: \
Replace database-1.***.***.rds.amazonaws.com with your actual MySQL host. \
Change admin and admin123 to your actual MySQL username and password. \
Ensure the target MySQL database (gravitino) exists before deployment._

## Iceberg REST Service Configuration

The Gravitino Helm chart supports deploying the Iceberg REST service as an auxiliary service embedded within the main Gravitino server. This provides a unified metadata lake experience with Iceberg REST API support.

### Quick Start with Iceberg REST

#### Enable Iceberg REST Service

To enable the Iceberg REST service with default configuration:

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set auxService.names="iceberg-rest"
```

The Iceberg REST service will be accessible at `http://<gravitino-service>:9001/iceberg/`.

#### Verify Iceberg REST Service

```console
# Check pod status
kubectl get pods -n gravitino -l app=gravitino

# Port forward to test the service
kubectl port-forward -n gravitino service/gravitino 9001:9001

# Test the Iceberg REST API
curl http://localhost:9001/iceberg/v1/config
```

Expected response:
```json
{
  "defaults": {},
  "overrides": {}
}
```

### Common Iceberg REST Configurations

#### Memory Backend (Development/Testing)

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set auxService.names="iceberg-rest" \
  --set icebergRest.catalogBackend="memory" \
  --set icebergRest.warehouse="/tmp/warehouse"
```

#### PostgreSQL Backend (Production)

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set auxService.names="iceberg-rest" \
  --set icebergRest.catalogBackend="jdbc" \
  --set icebergRest.warehouse="oss://my-oss-bucket/warehouse/" \
  --set icebergRest.additionalConfigItems."gravitino\.iceberg-rest\.jdbc-driver"="org.postgresql.Driver" \
  --set icebergRest.additionalConfigItems."gravitino\.iceberg-rest\.uri"="jdbc:postgresql://postgres:5432/iceberg" \
  --set icebergRest.additionalConfigItems."gravitino\.iceberg-rest\.jdbc-user"="iceberg" \
  --set icebergRest.additionalConfigItems."gravitino\.iceberg-rest\.jdbc-password"="password123" \
  --set icebergRest.additionalConfigItems."gravitino\.iceberg-rest\.jdbc-initialize"="true"
```

#### Alibaba Cloud OSS Storage Support

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set auxService.names="iceberg-rest" \
  --set icebergRest.catalogBackend="jdbc" \
  --set icebergRest.warehouse="oss://my-oss-bucket/warehouse/" \
  --set icebergRest.additionalConfigItems."gravitino\.iceberg-rest\.io-impl"="org.apache.iceberg.aliyun.oss.OSSFileIO" \
  --set icebergRest.additionalConfigItems."gravitino\.iceberg-rest\.oss-endpoint"="https://oss-cn-hangzhou.aliyuncs.com" \
  --set icebergRest.additionalConfigItems."gravitino\.iceberg-rest\.oss-access-key-id"="your-access-key-id" \
  --set icebergRest.additionalConfigItems."gravitino\.iceberg-rest\.oss-access-key-secret"="your-access-key-secret" \
  --set icebergRest.coreSiteProperties."fs\.oss\.impl"="org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem" \
  --set icebergRest.coreSiteProperties."fs\.oss\.endpoint"="oss-cn-hangzhou.aliyuncs.com" \
  --set icebergRest.coreSiteProperties."fs\.oss\.accessKeyId"="your-access-key-id" \
  --set icebergRest.coreSiteProperties."fs\.oss\.accessKeySecret"="your-access-key-secret"
```

### Service Exposure Options

#### Expose via LoadBalancer

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set auxService.names="iceberg-rest" \
  --set icebergRest.service.enabled=true \
  --set icebergRest.service.type="LoadBalancer"
```

#### Expose via Ingress

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set auxService.names="iceberg-rest" \
  --set icebergRest.service.enabled=true \
  --set icebergRest.ingress.enabled=true \
  --set icebergRest.ingress.hosts[0].host="iceberg-rest.example.com" \
  --set icebergRest.ingress.hosts[0].paths[0].path="/" \
  --set icebergRest.ingress.hosts[0].paths[0].pathType="ImplementationSpecific"
```

### Enable Health Checks

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set auxService.names="iceberg-rest" \
  --set icebergRest.readinessProbe.enabled=true \
  --set icebergRest.livenessProbe.enabled=true
```

### Performance Tuning

```console
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace \
  --set auxService.names="iceberg-rest" \
  --set icebergRest.minThreads=32 \
  --set icebergRest.maxThreads=256 \
  --set icebergRest.threadPoolWorkQueueSize=500
```

### Advanced Configuration with Values File

For complex configurations, create a custom values.yaml file:

```yaml
# iceberg-rest-oss-values.yaml
auxService:
  names: iceberg-rest

icebergRest:
  catalogBackend: jdbc
  warehouse: oss://my-oss-bucket/warehouse/
  
  # Performance settings
  minThreads: 32
  maxThreads: 256
  threadPoolWorkQueueSize: 500
  
  # Backend configuration
  additionalConfigItems:
    gravitino.iceberg-rest.jdbc-driver: org.postgresql.Driver
    gravitino.iceberg-rest.uri: jdbc:postgresql://postgres:5432/iceberg
    gravitino.iceberg-rest.jdbc-user: iceberg
    gravitino.iceberg-rest.jdbc-password: password123
    gravitino.iceberg-rest.jdbc-initialize: true
    gravitino.iceberg-rest.io-impl: org.apache.iceberg.aliyun.oss.OSSFileIO
    gravitino.iceberg-rest.oss-endpoint: https://oss-cn-hangzhou.aliyuncs.com
    gravitino.iceberg-rest.oss-access-key-id: ${OSS_ACCESS_KEY_ID}
    gravitino.iceberg-rest.oss-access-key-secret: ${OSS_ACCESS_KEY_SECRET}
  
  # Service exposure
  service:
    enabled: true
    type: ClusterIP
    port: 9001
  
  # Ingress configuration
  ingress:
    enabled: false
    className: "nginx"
    annotations: {}
    hosts:
      - host: gravitino-iceberg-rest.local
        paths:
          - path: /
            pathType: ImplementationSpecific
    tls: []
  
  # Health checks (sidecar container)
  readinessProbe:
    enabled: true
    initialDelaySeconds: 30
    timeoutSeconds: 5
    periodSeconds: 10
  livenessProbe:
    enabled: true
    initialDelaySeconds: 60
    timeoutSeconds: 5
    periodSeconds: 30
```

Deploy with the custom configuration:

```console
# Create secret for OSS credentials
kubectl create secret generic oss-credentials -n gravitino \
  --from-literal=OSS_ACCESS_KEY_ID=your-access-key-id \
  --from-literal=OSS_ACCESS_KEY_SECRET=your-access-key-secret

# Deploy with OSS configuration
helm upgrade --install gravitino ./gravitino -n gravitino --create-namespace -f iceberg-rest-oss-values.yaml
```

### Using Iceberg REST API

After deployment, you can interact with the Iceberg REST service:

```console
# Create a namespace
curl -X POST http://localhost:9001/iceberg/v1/namespaces \
  -H "Content-Type: application/json" \
  -d '{"namespace": ["test_db"]}'

# List namespaces
curl http://localhost:9001/iceberg/v1/namespaces

# Create a table
curl -X POST http://localhost:9001/iceberg/v1/namespaces/test_db/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test_table",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "id", "type": "long", "required": true},
        {"id": 2, "name": "name", "type": "string", "required": false}
      ]
    }
  }'

# List tables
curl http://localhost:9001/iceberg/v1/namespaces/test_db/tables
```

## Troubleshooting

### Common Issues

#### Iceberg REST Service Not Starting

1. Check if auxiliary service is enabled:
   ```console
   kubectl logs -n gravitino <pod-name> -c gravitino | grep "iceberg-rest"
   ```

2. Verify configuration:
   ```console
   kubectl exec -n gravitino <pod-name> -c gravitino -- cat /root/gravitino/conf/gravitino.conf | grep iceberg-rest
   ```

#### Health Check Failures

1. Check sidecar container logs:
   ```console
   kubectl logs -n gravitino <pod-name> -c iceberg-rest-health-check
   ```

2. Verify service endpoint:
   ```console
   kubectl exec -n gravitino <pod-name> -c gravitino -- curl http://localhost:9001/iceberg/v1/config
   ```

#### Configuration Issues

1. Check configuration file mounting:
   ```console
   kubectl exec -n gravitino <pod-name> -c gravitino -- ls -la /root/gravitino/iceberg-rest-server/conf/
   ```

2. Verify ConfigMap content:
   ```console
   kubectl describe configmap -n gravitino
   ```

#### Database Connection Issues

1. Test database connectivity:
   ```console
   kubectl exec -n gravitino <pod-name> -c gravitino -- nc -zv <db-host> <db-port>
   ```

2. Check JDBC configuration:
   ```console
   kubectl exec -n gravitino <pod-name> -c gravitino -- grep "jdbc" /root/gravitino/conf/gravitino.conf
   ```

#### OSS Storage Access Issues

1. Test OSS endpoint connectivity:
   ```console
   kubectl exec -n gravitino <pod-name> -c gravitino -- ping oss-cn-hangzhou.aliyuncs.com
   ```

2. Check OSS credentials:
   ```console
   kubectl exec -n gravitino <pod-name> -c gravitino -- env | grep OSS
   ```

3. Verify OSS configuration:
   ```console
   kubectl exec -n gravitino <pod-name> -c gravitino -- grep "oss" /root/gravitino/conf/gravitino.conf
   ```

4. Test OSS bucket access:
   ```console
   kubectl exec -n gravitino <pod-name> -c gravitino -- curl -I "https://your-bucket.oss-cn-hangzhou.aliyuncs.com/"
   ```

5. Check OSS Hadoop configuration:
   ```console
   kubectl exec -n gravitino <pod-name> -c gravitino -- cat /root/gravitino/iceberg-rest-server/conf/hadoop/core-site.xml | grep oss
   ```

### Debug Commands

```console
# Get all resources
kubectl get all -n gravitino -l app.kubernetes.io/name=gravitino

# Check pod status
kubectl describe pod -n gravitino <pod-name>

# View logs
kubectl logs -n gravitino <pod-name> -c gravitino

# Test API connectivity
kubectl port-forward -n gravitino service/gravitino 9001:9001
curl http://localhost:9001/iceberg/v1/config

# Check secrets
kubectl get secrets -n gravitino
kubectl describe secret oss-credentials -n gravitino

# Verify environment variables
kubectl exec -n gravitino <pod-name> -c gravitino -- printenv | grep -E "(OSS|ICEBERG)"
```

## Uninstall Helm Chart

```console
helm uninstall [RELEASE_NAME] -n [NAMESPACE]
```

To completely clean up all resources:

```console
# Uninstall the chart
helm uninstall gravitino -n gravitino

# Clean up OSS credentials secret
kubectl delete secret oss-credentials -n gravitino

# Clean up any other secrets (if created)
kubectl delete secret postgres-credentials -n gravitino

# Delete persistent volume claims (if any)
kubectl delete pvc -n gravitino --all

# Delete the namespace (if desired)
kubectl delete namespace gravitino
```

## Additional Resources

- [Iceberg REST Service Documentation](iceberg-rest-service.md)
- [Gravitino Server Configuration](gravitino-server-config.md)
- [How to Build Gravitino](how-to-build.md)
- [How to Install Gravitino](how-to-install.md)
- [Alibaba Cloud OSS Documentation](https://www.alibabacloud.com/help/en/oss)
```
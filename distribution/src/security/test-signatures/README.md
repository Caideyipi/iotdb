# Preparing the keys for the module signing

We're using the official Ignition key-signing tool, but wrapping it in a maven plugin execution for simplicity.

In order for this to work, we need to generate some keys. 
If you follow the below steps exactly, you should be up and running in no time.

The default uses a password of `password` for the key signing key, so if you use this as password for all steps above, no changes should be required.

Also be sure to execute these steps in this directory, so all generated files will be located alongside this README.md

## use KeyStore Explorer to generate a self-signed certificate
https://keystore-explorer.org/downloads.html
![img.png](img.png)

## Generate a self-signed CA certificate

```
openssl genrsa -des3 -out server.key 4096

openssl req -new -key server.key -out server.csr

openssl x509 -req -days 36500 -in server.csr -signkey server.key -out server.crt

openssl crl2pkcs7 -nocrl -certfile server.crt -out server.p7b -certfile server.crt
```
## Generate a key-signing certificate
```
openssl genrsa -des3 -out code-signing.key 4096

openssl req -new -key code-signing.key -out code-signing.csr

openssl x509 -req -in code-signing.csr -CA server.crt -CAkey server.key -CAcreateserial -out code-signing.pem -days 36500 -sha256

openssl pkcs12 -inkey code-signing.key -in code-signing.pem -export -out code-signing.p12

keytool -importkeystore -srckeystore code-signing.p12 -srcstoretype pkcs12 -destkeystore code-signing.jks
```



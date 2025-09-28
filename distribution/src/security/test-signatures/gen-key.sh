keytool -genkeypair \
  -alias TCDBTCS \
  -keyalg RSA \
  -keysize 4096 \
  -validity 3650 \
  -keystore code-signing.jks \
  -storepass "Timecho2021" \
  -keypass "Timecho2021" \
  -dname "CN=Timecho Test Code Signing, OU=Development, O=Timecho, L=Beijing, ST=Beijing, C=CN"
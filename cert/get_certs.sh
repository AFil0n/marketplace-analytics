#!/bin/bash

function create_certs() {
    set -e

    trap 'echo -e "\n\033[31mОШИБКА! Скрипт остановлен.\033[0m"; read -p "Нажмите Enter, чтобы закрыть терминал..."' ERR
    echo "Запуск создания CA!"
    openssl req -new -nodes -x509 -days 365 -newkey rsa:2048 -keyout ca.key -out ca.crt -config ca.cnf
    cat ca.crt ca.key > ca.pem

    echo "Создаем сертификаты для controller"
    mkdir -p "controller-creds"

    echo "Копируем kafka_server_jaas.conf"
    cp kafka_server_jaas.conf controller-creds/

    # Создаем временный конфиг для controller с правильными SAN
    cat > controller.cnf << EOF
[ req ]
default_bits            = 2048
default_md              = sha256
prompt                  = no
distinguished_name      = dn
req_extensions          = v3_req
utf8                    = yes
encrypt_key             = no

[ dn ]
countryName             = RU
stateOrProvinceName     = Moscow
localityName            = Moscow
organizationName        = Yandex
organizationalUnitName  = Practice
commonName              = kafka-controller-0

[ v3_req ]
basicConstraints        = CA:FALSE
keyUsage                = digitalSignature, keyEncipherment
extendedKeyUsage        = serverAuth, clientAuth
subjectAltName          = DNS:kafka-controller-0, DNS:localhost, IP:127.0.0.1
nsComment               = "Kafka Controller Certificate"
EOF

    echo "1. Генерация ключа и CSR для controller"
    openssl req -new -newkey rsa:2048 \
        -keyout controller-creds/kafka-controller.key \
        -out controller-creds/kafka-controller.csr \
        -config controller.cnf -nodes

    echo "2. Подписание сертификата CA для controller"
    openssl x509 -req -days 365 \
        -in controller-creds/kafka-controller.csr \
        -CA ca.crt -CAkey ca.key \
        -CAcreateserial \
        -out controller-creds/kafka-controller.crt \
        -extfile controller.cnf -extensions v3_req

    echo "3. Создание полной цепочки сертификатов"
    cat controller-creds/kafka-controller.crt ca.crt > controller-creds/kafka-controller-fullchain.crt

    echo "4. Создание PKCS12 хранилища"
    openssl pkcs12 -export \
        -in controller-creds/kafka-controller-fullchain.crt \
        -inkey controller-creds/kafka-controller.key \
        -name "kafka-controller" \
        -out controller-creds/kafka.keystore.pkcs12 \
        -passout pass:password \
        -certfile ca.crt

    echo "5. Создание Truststore в формате PKCS12"
    keytool -import -trustcacerts \
        -file ca.crt \
        -alias ca \
        -keystore controller-creds/kafka.truststore.jks \
        -storepass password \
        -noprompt

    echo "6. Сохранение паролей"
    echo "password" > controller-creds/kafka-controller_sslkey_creds
    echo "password" > controller-creds/kafka-controller_keystore_creds
    echo "password" > controller-creds/kafka-controller_truststore_creds

    for i in 0 1; do
        echo "Создаем сертификаты для kafka-$i"
        mkdir -p "kafka-$i-creds"

        echo "Копируем kafka_server_jaas.conf"
        cp kafka_server_jaas.conf kafka-$i-creds/

        # Создаем конфиг для каждого брокера с правильными SAN
        cat > kafka-$i.cnf << EOF
[ req ]
default_bits            = 2048
default_md              = sha256
prompt                  = no
distinguished_name      = dn
req_extensions          = v3_req
utf8                    = yes
encrypt_key             = no

[ dn ]
countryName             = RU
stateOrProvinceName     = Moscow
localityName            = Moscow
organizationName        = Yandex
organizationalUnitName  = Practice
commonName              = kafka-$i

[ v3_req ]
basicConstraints        = CA:FALSE
keyUsage                = digitalSignature, keyEncipherment
extendedKeyUsage        = serverAuth, clientAuth
subjectAltName          = DNS:kafka-$i, DNS:localhost, IP:127.0.0.1
nsComment               = "Kafka Broker Certificate"
EOF

        echo "1. Генерация ключа и CSR для kafka-$i"
        openssl req -new -newkey rsa:2048 \
            -keyout kafka-$i-creds/kafka-$i.key \
            -out kafka-$i-creds/kafka-$i.csr \
            -config kafka-$i.cnf -nodes

        echo "2. Подписание сертификата CA для kafka-$i"
        openssl x509 -req -days 365 \
            -in kafka-$i-creds/kafka-$i.csr \
            -CA ca.crt -CAkey ca.key \
            -CAcreateserial \
            -out kafka-$i-creds/kafka-$i.crt \
            -extfile kafka-$i.cnf -extensions v3_req

        echo "3. Создание полной цепочки сертификатов"
        cat kafka-$i-creds/kafka-$i.crt ca.crt > kafka-$i-creds/kafka-$i-fullchain.crt

        echo "4. Создание PKCS12 хранилища (keystore)"
        openssl pkcs12 -export \
            -in kafka-$i-creds/kafka-$i-fullchain.crt \
            -inkey kafka-$i-creds/kafka-$i.key \
            -name "kafka-$i" \
            -out kafka-$i-creds/kafka.keystore.pkcs12 \
            -passout pass:password \
            -certfile ca.crt

        echo "5. Создание Truststore в формате PKCS12"
        keytool -import -trustcacerts \
            -file ca.crt \
            -alias ca \
            -keystore kafka-$i-creds/kafka.truststore.jks \
            -storepass password \
            -noprompt

        echo "6. Сохранение паролей"
        echo "password" > kafka-$i-creds/kafka-${i}_sslkey_creds
        echo "password" > kafka-$i-creds/kafka-${i}_keystore_creds
        echo "password" > kafka-$i-creds/kafka-${i}_truststore_creds

        # Удаляем временный конфиг
        rm kafka-$i.cnf
    done

    # Удаляем временные конфиги
    rm controller.cnf

    echo "Сертификаты успешно созданы!"
    read -p "Нажмите Enter для выхода..."
}

create_certs
FROM artifactory.global.standardchartered.com/gv-images-products/bi/ubi9/ubi-minimal:9.6-1752587672-7223582
EXPOSE 8443
WORKDIR /
COPY build/linux-amd64/gdp-api .
USER 65532:65532
ENTRYPOINT [ "./gdp-api" ]

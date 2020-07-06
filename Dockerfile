FROM kpavel/pywren-test
COPY pywren_ibmcf.zip pywren_ibmcf.zip
RUN unzip pywren_ibmcf.zip -d /action

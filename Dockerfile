LABEL org.opencontainers.image.source = "https://github.com/robshakir/rtlamr"
FROM docker.io/dtcooper/raspberrypi-os:bookworm 

RUN apt-get update && \
    apt-get install -y rtl-sdr supervisor && \
    apt-get clean
 
WORKDIR /
EXPOSE 1234
COPY docker/supervisord.conf /etc/supervisor/supervisord.conf
COPY docker/rtlamr.conf /etc/supervisor/conf.d/rtlamr.conf
COPY docker/rtltcp.conf /etc/supervisor/conf.d/rtltcp.conf
COPY rtlamr /rtlamr
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]

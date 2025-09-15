# Energy2mqtt build system

# we use the rust build container
FROM docker.io/rust:latest as builder

# Copy our need files and run cargo build and install
WORKDIR /usr/src/energy2mqtt
COPY src src
COPY Cargo.toml .
RUN cargo install --path . --root /build/usr

# Copy the files which are static for now
COPY defs /build/defs
COPY ui /build/ui

# create the linux default directories
RUN mkdir /build/{sys,proc,root,mnt,home}

# In the last step we want to evaluate all files needed
# by our binary and only copy those. That way we save
# space by only including the stuff we need
COPY scripts/copy_libs.sh /root/

WORKDIR /build/
RUN /root/copy_libs.sh /build/usr/bin/energy2mqtt

# Start with an emtpy file system as we do not really
# need any files inside our container
FROM scratch

# Populate the rootfs with our files
WORKDIR /
COPY --from=builder /build/ /
# /config is volataile for our configration
# use docker volume or bind mount to allow changes
VOLUME /config
ENTRYPOINT [ "/usr/bin/energy2mqtt" ]


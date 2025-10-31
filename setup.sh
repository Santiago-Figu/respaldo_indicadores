# build and up services
docker compose up -d

# prune system
docker system prune -a -f

# remove unused volumes
docker volume prune -f
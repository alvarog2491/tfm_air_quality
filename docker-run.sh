#!/bin/bash

# Air Quality Analysis Pipeline - Docker Runner Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_help() {
    echo "Air Quality Analysis Pipeline - Docker Runner"
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  full          Run complete pipeline (ETL → Modeling → App)"
    echo "  etl           Run only ETL pipeline"
    echo "  modeling      Run only modeling pipeline"
    echo "  app           Run only Flask application"
    echo "  monitoring    Run with monitoring dashboard"
    echo "  build         Build all Docker images"
    echo "  clean         Clean up containers and images"
    echo "  logs          Show logs for all services"
    echo "  status        Show status of all services"
    echo ""
    echo "Options:"
    echo "  --build       Force rebuild images"
    echo "  --detach      Run in background"
    echo "  --follow      Follow logs (with logs command)"
    echo ""
    echo "Examples:"
    echo "  $0 full --build          # Build and run complete pipeline"
    echo "  $0 etl --detach          # Run ETL in background"
    echo "  $0 logs --follow         # Follow all logs"
    echo "  $0 app                   # Run only the Flask app"
}

create_directories() {
    echo -e "${BLUE}Creating necessary directories...${NC}"
    mkdir -p logs/etl logs/modeling logs/app
    mkdir -p data models metrics
    echo -e "${GREEN}Directories created.${NC}"
}

build_images() {
    echo -e "${BLUE}Building Docker images...${NC}"
    docker-compose build --parallel
    echo -e "${GREEN}Images built successfully.${NC}"
}

run_service() {
    local profile=$1
    local options=$2
    
    echo -e "${BLUE}Running $profile service(s)...${NC}"
    
    create_directories
    
    if [[ $options == *"--build"* ]]; then
        build_images
    fi
    
    if [[ $options == *"--detach"* ]]; then
        docker-compose --profile $profile up -d
        echo -e "${GREEN}Services started in background.${NC}"
        echo -e "${YELLOW}Use '$0 logs' to view logs or '$0 status' to check status.${NC}"
    else
        docker-compose --profile $profile up
    fi
}

show_logs() {
    local options=$1
    if [[ $options == *"--follow"* ]]; then
        docker-compose logs -f
    else
        docker-compose logs
    fi
}

show_status() {
    echo -e "${BLUE}Service Status:${NC}"
    docker-compose ps
    echo ""
    echo -e "${BLUE}Network Information:${NC}"
    docker network ls | grep tfm || echo "No TFM networks found"
    echo ""
    echo -e "${BLUE}Volume Information:${NC}"
    docker volume ls | grep tfm || echo "No TFM volumes found"
}

clean_up() {
    echo -e "${YELLOW}Cleaning up containers and images...${NC}"
    docker-compose down -v --remove-orphans
    docker system prune -f
    echo -e "${GREEN}Cleanup completed.${NC}"
}

# Main script logic
case "$1" in
    "full")
        run_service "full" "$2"
        ;;
    "etl")
        run_service "etl" "$2"
        ;;
    "modeling")
        run_service "modeling" "$2"
        ;;
    "app")
        run_service "app" "$2"
        ;;
    "monitoring")
        run_service "monitoring" "$2"
        ;;
    "build")
        build_images
        ;;
    "logs")
        show_logs "$2"
        ;;
    "status")
        show_status
        ;;
    "clean")
        clean_up
        ;;
    "help"|"-h"|"--help")
        print_help
        ;;
    *)
        echo -e "${RED}Invalid command: $1${NC}"
        echo ""
        print_help
        exit 1
        ;;
esac
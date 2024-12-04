#!/bin/bash

export RED='\033[31m'
export GREEN='\033[32m'
export YELLOW='\033[33m'
export BLUE='\033[34m'
export MAGENTA='\033[35m'
export CYAN='\033[36m'
export RESET='\033[0m'

echo -e "${GREEN}=====================================================================${RESET}"
echo -e "${GREEN}=====================================================================${RESET}"
echo -e "${GREEN}============ Welcome to the Dinky initialization script =============${RESET}"
echo -e "${GREEN}======================================================================${RESET}"
echo -e "${GREEN}======================================================================${RESET}"

APP_HOME=${DINKY_HOME:-$(cd "$(dirname "$0")"; cd ..; pwd)}
export DINKY_HOME=${APP_HOME}

sudo chmod +x "${APP_HOME}"/bin/init_*.sh



EXTENDS_HOME="${APP_HOME}/extends"
if [ ! -d "${EXTENDS_HOME}" ]; then
    echo -e "${RED} ${EXTENDS_HOME} Directory does not exist, please check${RESET}"
    exit 1
fi

FLINK_VERSION_SCAN=$(ls -n "${EXTENDS_HOME}" | grep '^d' | grep flink | awk -F 'flink' '{print $2}')
if [ -z "${FLINK_VERSION_SCAN}" ]; then
    echo -e "${RED}There is no Flink related version in ${EXTENDS_HOME} in the directory where Dinky is deployed. The initialization operation cannot be performed. Please check. ${RESET}"
    exit 1
fi

DINKY_TMP_DIR="${APP_HOME}/tmp"
if [ ! -d "${DINKY_TMP_DIR}" ]; then
    echo -e "${YELLOW}Create temporary directory ${DINKY_TMP_DIR}...${RESET}"
    mkdir -p "${DINKY_TMP_DIR}"
    echo -e "${GREEN}The temporary directory is created${RESET}"
fi

# LIB
DINKY_LIB="${APP_HOME}/lib"
if [ ! -d "${DINKY_LIB}" ]; then
    echo -e "${RED}${DINKY_LIB} Directory does not exist, please check. ${RESET}"
    exit 1
fi

# 函数：检查命令是否存在，不存在则尝试安装
check_command() {
    local cmd="$1"
    echo -e "${BLUE}Check if command: $cmd exists...${RESET}"
    if ! command -v "$cmd" &> /dev/null; then
        if [ "$cmd" == "yum" ]; then
            echo -e "${YELLOW} Try using yum to install the missing command...${RESET}"
            sudo yum install -y "$cmd"
        elif [ "$cmd" == "apt-get" ]; then
            echo -e "${YELLOW}Try using apt-get to install the missing command...${RESET}"
            sudo apt-get install -y "$cmd"
        else
            echo -e "${RED} $cmd The command was not found. Please install it manually and then run this script.。${RESET}"
            exit 1
        fi
    fi
    echo -e "${GREEN}========== Command $cmd check completed. OK, continue executing the script. ==========${RESET}"
}

sh "${APP_HOME}/bin/init_check_network.sh"

check_command "wget"

echo -e "${GREEN}The pre-check is completed. Welcome to use the Dinky initialization script. The current Dinky root path is：${APP_HOME} ${RESET}"

function download_file() {
    source_url=$1
    target_file_dir=$2
    echo -e "${GREEN}Start downloading $source_url to $target_file_dir...${RESET}"
    wget -P "${target_file_dir}" "${source_url}"
    echo -e "${GREEN}Download completed. The downloaded file storage address is: $target_file_dir ${RESET}"
}

export -f download_file

echo
echo
echo -e "${GREEN} ====================== Data source driver initialization script -> Start ====================== ${RESET}"

while true; do
    echo -e "${BLUE} ========================= Please enter your database type ================================ ${RESET}"
    echo -e "${BLUE} ======== (h2 comes with it by default and does not need to perform this step)===========  ${RESET}"
    echo -e "${BLUE} ============================== Please select 1, 2, 3 ======================================  ${RESET}"
    echo -e "${BLUE} ==================================== 1. mysql =============================================  ${RESET}"
    echo -e "${BLUE} ==================================== 2. pgsql =========================================  ${RESET}"
    echo -e "${BLUE} ================================ 3. Skip this step ==========================================  ${RESET}"
    echo -e "${BLUE} ================================ Enter number selection ==================================  ${RESET}"
    read -p "Please enter your database type：" db_type
    case $db_type in
        1)
            echo -e "${GREEN}Start downloading the mysql driver package...${RESET}"
            if [ -f "${DINKY_LIB}/mysql-connector-j-8.4.0.jar" ]; then
                echo -e "${GREEN}mysql The driver package already exists, no need to download it again. Skip this step。${RESET}"
            else
                download_file https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar "${DINKY_LIB}"
                echo -e "${GREEN}Download is complete, please verify. The downloaded file storage address is： ${DINKY_LIB}/mysql-connector-j-8.4.0.jar${RESET}"
                if [ -f "${DINKY_LIB}/mysql-connector-j-8.4.0.jar" ]; then
                    echo -e "${GREEN}mysql driver package downloaded successfully。${RESET}"
                else
                    echo -e "${RED}Mysql driver package download failed, please check the network or download manually。${RESET}"
                    exit 1
                fi
                echo -e "${GREEN}After the verification is completed, subsequent installation and configuration operations can be performed as needed.。${RESET}"
            fi
            break
            ;;
        2)
            echo -e "${GREEN}It seems that pgsql has been integrated by default, so there is no need to perform this step. Please perform subsequent installation and configuration operations as needed.${RESET}"
            break
            ;;
        3)
            echo -e "${GREEN}Skip this step。${RESET}"
            break
            ;;
        *)
            echo -e "${RED}The entered database type is incorrect, please rerun the script to select the correct database type.${RESET}"
            ;;
    esac
done
echo -e "${GREEN} ====================== Data source driver initialization script -> end====================== ${RESET}"

echo
echo

echo -e "${GREEN} ====================== Flink depends on initialization script -> start ====================== ${RESET}"

declare -A version_map
version_map["1.14"]="1.14.6"
version_map["1.15"]="1.15.4"
version_map["1.16"]="1.16.3"
version_map["1.17"]="1.17.2"
version_map["1.18"]="1.18.1"
version_map["1.19"]="1.19.1"
version_map["1.20"]="1.20.0"

FLINK_VERSION_SCAN=$(ls -n "${EXTENDS_HOME}" | grep '^d' | grep flink | awk -F 'flink' '{print $2}')
if [ -z "${FLINK_VERSION_SCAN}" ]; then
    echo -e "${RED}There is no Flink related version in ${EXTENDS_HOME} in the directory where Dinky is deployed. The initialization operation cannot be performed. Please check.${RESET}"
    exit 1
else
    echo -e "${GREEN}The current Flink version number deployed by Dinky:${FLINK_VERSION_SCAN}${RESET}"
fi

# 根据 Dinky 部署的Flink对应的版本号，获取对应的 Flink 版本
CURRENT_FLINK_FULL_VERSION=${version_map[$FLINK_VERSION_SCAN]}

echo -e "${GREEN}Obtain the version number corresponding to the deployed Flink (full version number) based on the scanned current Flink version number: flink-${CURRENT_FLINK_FULL_VERSION}${RESET}"

# 步骤2：获取Dinky部署的Flink对应的版本号，然后下载Flink安装包
while true; do
    read -p "It is detected that the Flink version number deployed by Dinky is: ${FLINK_VERSION_SCAN}, and the Flink installation package version number that needs to be downloaded is: flink-${CURRENT_FLINK_FULL_VERSION}-bin-scala_2.12.tgz. Please choose whether to initialize Flink related dependencies?（yes/no/exit）" is_init_flink
    is_init_flink=$(echo "$is_init_flink" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')

    case $is_init_flink in
        yes | y )
            sh "${APP_HOME}"/bin/init_flink_dependences.sh "${CURRENT_FLINK_FULL_VERSION}" "${FLINK_VERSION_SCAN}" "${DINKY_TMP_DIR}" "${EXTENDS_HOME}" "${APP_HOME}"
            break
            ;;
        no | n )
            echo -e "${GREEN}The Flink installation package download operation has been skipped. Please download manually${RESET}"
            break
            ;;
        exit | e )
            echo -e "${GREEN}If you choose exit, the program will exit。${RESET}"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid input, please re-enter yes/no/exit。${RESET}"
            ;;
    esac
done
echo -e "${GREEN} ====================== Flink depends on initialization script -> end ====================== ${RESET}"

echo
echo

echo -e "${GREEN} ====================== Hadoop dependency initialization script -> Start ====================== ${RESET}"

while true; do
    read -p "Is your deployment environment a Hadoop environment?？（yes/no/exit）" is_hadoop
    is_hadoop=$(echo "$is_hadoop" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')
    case $is_hadoop in
        yes | y )
            sh "${APP_HOME}/bin/init_hadoop_dependences.sh" "${EXTENDS_HOME}"
            break
            ;;
        no | n )
            echo -e "${GREEN}Hadoop related operations skipped ${RESET}"
            break
            ;;
        exit | e )
            echo -e "${GREEN}If you choose exit, the program will exit${RESET}"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid input, please re-enter yes/no/exit。${RESET}"
            ;;
    esac
done
echo -e "${GREEN} ======================Hadoop dependency initialization script -> end ====================== ${RESET}"
echo

echo -e "${GREEN} === After the environment initialization is completed, you can configure the application configuration file in Dinky's config directory to perform database-related configuration, or execute the initialization configuration file.。====  ${RESET}"
echo

echo -e "${GREEN} ====================== Database configuration file initialization script -> Start ====================== ${RESET}"

while true; do
    read -p "Do you need to initialize the database configuration file?？(yes/no)：" is_init_db
    is_init_db=$(echo "$is_init_db" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')
    case $is_init_db in
        yes | y )
            sh "${APP_HOME}/bin/init_db.sh" "${DINKY_HOME}"
            break
            ;;
        no | n )
            echo -e "${GREEN}The database initialization operation has been skipped, please manually configure the database ${DINKY_HOME}/config/application.yml file and ${DINKY_HOME}/config/application-[mysql/pgsql].yml file。${RESET}"
            break
            ;;
        exit | e )
            echo -e "${GREEN}The script has exited, please manually configure the database ${DINKY_HOME}/config/application.yml file and ${DINKY_HOME}/config/application-[mysql/pgsql].yml file。${RESET}"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid input, please re-enter yes/no/exit。${RESET}"
            ;;
    esac
done
echo -e "${GREEN} ====================== Database configuration file initialization script -> End ====================== ${RESET}"
#!/bin/bash
# setup_hadoop_gpu.sh — One-shot Hadoop pseudo-distributed setup for the GPU server.
#
# Uses non-default ports to avoid conflicts with other users on the same machine:
#   HDFS NameNode  : 19000  (default 9000)
#   YARN RM RPC    : 18032  (default 8032)
#   YARN RM webapp : 18088  (default 8088)
#   YARN NM webapp : 18042  (default 8042)
#
# Assumes:
#   - Java 11 is installed at /usr/lib/jvm/java-11-openjdk-amd64
#   - Hadoop 3.3.6 is already extracted at ~/Krish/nosql/hadoop-3.3.6
#
# Run once:
#   chmod +x setup_hadoop_gpu.sh
#   ./setup_hadoop_gpu.sh
#
set -e

JAVA_HOME_PATH="/usr/lib/jvm/java-11-openjdk-amd64"
HADOOP_HOME_PATH="$HOME/Krish/nosql/hadoop-3.3.6"
HADOOP_DATA="$HOME/Krish/nosql/hadoop-data"
HADOOP_CONF="$HADOOP_HOME_PATH/etc/hadoop"

HDFS_PORT=19000
YARN_RM_PORT=18032
YARN_RM_WEBAPP_PORT=18088
YARN_NM_WEBAPP_PORT=18042

# ---------------------------------------------------------------------------
# 0. Sanity checks
# ---------------------------------------------------------------------------
echo ""
echo "=== [0] Checking prerequisites ==="

if [ ! -d "$HADOOP_HOME_PATH" ]; then
    echo "ERROR: Hadoop not found at $HADOOP_HOME_PATH"
    exit 1
fi
if [ ! -d "$JAVA_HOME_PATH" ]; then
    echo "ERROR: Java 11 not found at $JAVA_HOME_PATH"
    echo "Run: sudo apt install -y openjdk-11-jdk"
    exit 1
fi

echo "Hadoop : $HADOOP_HOME_PATH  OK"
echo "Java   : $JAVA_HOME_PATH  OK"

# ---------------------------------------------------------------------------
# 1. Environment variables in ~/.bashrc
# ---------------------------------------------------------------------------
echo ""
echo "=== [1] Setting environment variables in ~/.bashrc ==="

add_if_missing() {
    grep -qF "$1" ~/.bashrc || echo "$1" >> ~/.bashrc
}

add_if_missing "export JAVA_HOME=$JAVA_HOME_PATH"
add_if_missing "export HADOOP_HOME=$HADOOP_HOME_PATH"
add_if_missing "export PATH=\$JAVA_HOME/bin:\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin"

export JAVA_HOME="$JAVA_HOME_PATH"
export HADOOP_HOME="$HADOOP_HOME_PATH"
export PATH="$JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"

echo "Done."

# ---------------------------------------------------------------------------
# 2. Set JAVA_HOME inside Hadoop
# ---------------------------------------------------------------------------
echo ""
echo "=== [2] Configuring JAVA_HOME in hadoop-env.sh ==="

grep -qF "export JAVA_HOME=$JAVA_HOME_PATH" "$HADOOP_CONF/hadoop-env.sh" \
    || echo "export JAVA_HOME=$JAVA_HOME_PATH" >> "$HADOOP_CONF/hadoop-env.sh"

echo "Done."

# ---------------------------------------------------------------------------
# 3. Write the 4 XML config files (with alternate ports)
# ---------------------------------------------------------------------------
echo ""
echo "=== [3] Writing Hadoop XML configs (ports: HDFS=$HDFS_PORT, YARN=$YARN_RM_PORT/$YARN_RM_WEBAPP_PORT) ==="

mkdir -p "$HADOOP_DATA/namenode" "$HADOOP_DATA/datanode"

# core-site.xml
cat > "$HADOOP_CONF/core-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:$HDFS_PORT</value>
  </property>
</configuration>
EOF

# hdfs-site.xml
cat > "$HADOOP_CONF/hdfs-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>$HADOOP_DATA/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>$HADOOP_DATA/datanode</value>
  </property>
  <property>
    <name>dfs.namenode.http-address</name>
    <value>localhost:19870</value>
  </property>
  <property>
    <name>dfs.namenode.secondary.http-address</name>
    <value>localhost:19868</value>
  </property>
  <property>
    <name>dfs.datanode.address</name>
    <value>localhost:19866</value>
  </property>
  <property>
    <name>dfs.datanode.http.address</name>
    <value>localhost:19864</value>
  </property>
  <property>
    <name>dfs.datanode.ipc.address</name>
    <value>localhost:19867</value>
  </property>
</configuration>
EOF

# mapred-site.xml
cat > "$HADOOP_CONF/mapred-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME_PATH</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME_PATH</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME_PATH</value>
  </property>
</configuration>
EOF

# yarn-site.xml
cat > "$HADOOP_CONF/yarn-site.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>localhost:$YARN_RM_PORT</value>
  </property>
  <property>
    <name>yarn.resourcemanager.webapp.address</name>
    <value>localhost:$YARN_RM_WEBAPP_PORT</value>
  </property>
  <property>
    <name>yarn.nodemanager.webapp.address</name>
    <value>localhost:$YARN_NM_WEBAPP_PORT</value>
  </property>
</configuration>
EOF

echo "Done. Configs written to $HADOOP_CONF/"

# ---------------------------------------------------------------------------
# 4. Passwordless SSH to localhost
# ---------------------------------------------------------------------------
echo ""
echo "=== [4] Setting up passwordless SSH to localhost ==="

if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
    echo "SSH key generated."
else
    echo "SSH key already exists, skipping keygen."
fi

grep -qF "$(cat ~/.ssh/id_rsa.pub)" ~/.ssh/authorized_keys 2>/dev/null \
    || cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
ssh-keyscan -H localhost >> ~/.ssh/known_hosts 2>/dev/null

echo "Testing SSH to localhost..."
if ssh -o BatchMode=yes -o ConnectTimeout=5 localhost "echo 'SSH OK'" 2>/dev/null; then
    echo "SSH to localhost works."
else
    echo "WARNING: SSH to localhost failed. Make sure sshd is running."
    echo "Run: sudo systemctl start ssh"
    exit 1
fi

# ---------------------------------------------------------------------------
# 5. Check alternate ports are free
# ---------------------------------------------------------------------------
echo ""
echo "=== [5] Checking ports $HDFS_PORT, $YARN_RM_PORT, $YARN_RM_WEBAPP_PORT are free ==="

CONFLICT=0
for PORT in $HDFS_PORT $YARN_RM_PORT $YARN_RM_WEBAPP_PORT 19870 19868 19866 19864 19867; do
    if ss -tlnp 2>/dev/null | grep -q ":$PORT "; then
        echo "ERROR: Port $PORT is also in use. Change the port variables at the top of this script."
        CONFLICT=1
    fi
done

if [ "$CONFLICT" -eq 1 ]; then
    exit 1
fi

echo "All ports free."

# ---------------------------------------------------------------------------
# 6. Format HDFS and start daemons
# ---------------------------------------------------------------------------
echo ""
echo "=== [6] Formatting HDFS ==="
hdfs namenode -format -force

echo ""
echo "=== [7] Starting Hadoop daemons ==="
start-dfs.sh
start-yarn.sh

sleep 3

echo ""
echo "=== [8] Verifying — running jps ==="
jps

# ---------------------------------------------------------------------------
# 9. Create HDFS user directory
# ---------------------------------------------------------------------------
echo ""
echo "=== [9] Creating HDFS directories ==="
hdfs dfs -mkdir -p /user/$USER/input
hdfs dfs -mkdir -p /user/$USER/output
echo "Done."

echo ""
echo "======================================="
echo "Setup complete!"
echo "Ports in use: HDFS=$HDFS_PORT  YARN=$YARN_RM_PORT/$YARN_RM_WEBAPP_PORT"
echo ""
echo "--- Next: copy files to this server (run on your laptop) ---"
echo ""
echo "  scp problem2.jar $USER@<server-ip>:~/Krish/nosql/"
echo "  scp stopwords.txt $USER@<server-ip>:~/Krish/nosql/"
echo "  scp Wikipedia-EN-20120601_ARTICLES.tar.gz $USER@<server-ip>:~/Krish/nosql/"
echo ""
echo "--- Then on this server, upload dataset to HDFS ---"
echo ""
echo "  cd ~/Krish/nosql"
echo "  tar -xzf Wikipedia-EN-20120601_ARTICLES.tar.gz"
echo "  hdfs dfs -put Wikipedia-EN-20120601_ARTICLES /user/\$USER/input/"
echo "  hdfs dfs -put stopwords.txt /user/\$USER/"
echo ""
echo "--- Then run the jobs ---"
echo ""
echo "  # Problem 2a"
echo "  hadoop jar ~/Krish/nosql/problem2.jar parta.DocumentFrequency \\"
echo "    /user/\$USER/input/Wikipedia-EN-20120601_ARTICLES \\"
echo "    /user/\$USER/output/df_output_full \\"
echo "    /user/\$USER/stopwords.txt"
echo ""
echo "  # Extract top 100"
echo "  hdfs dfs -getmerge /user/\$USER/output/df_output_full /tmp/df_full.tsv"
echo "  sort -t\$'\\t' -k2 -rn /tmp/df_full.tsv | head -100 > ~/Krish/nosql/df_top100_full.tsv"
echo "  hdfs dfs -put ~/Krish/nosql/df_top100_full.tsv /user/\$USER/"
echo ""
echo "  # Problem 2b"
echo "  hadoop jar ~/Krish/nosql/problem2.jar partb.TFIDFScorer \\"
echo "    /user/\$USER/input/Wikipedia-EN-20120601_ARTICLES \\"
echo "    /user/\$USER/output/tfidf_output_full \\"
echo "    /user/\$USER/df_top100_full.tsv"
echo "======================================="

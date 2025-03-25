# Spark and Python using VSCODE 

1. Install Spark on your local machine

```bash
wget wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
tar -xvzf spark-3.5.5-bin-hadoop3.tgz
mv spark-3.5.5-bin-hadoop3 ~/spark
```

2. Export the path to your .bashrc or .zshrc file

```bash
export SPARK_HOME=~/spark
export PATH=$SPARK_HOME/bin:$PATH
```

3. Verify the installation

```bash
spark-shell
spark-submit
```

![spark-shell](./docs/img/spark-shell.png)

4. Install prettytable

```bash
pip install prettytable
```

5. Run self-contained applications

```bash
spark-submit main.py
```

![spark-submit](./docs/img/spark-submit.png)
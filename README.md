# Série de exemplos usando Spark 
**Este repositório foi criado para fazer a implementação em python e em scala do curso [Apache Spark with Scala - Hands On with Big Data!](https://www.udemy.com/course/apache-spark-with-scala-hands-on-with-big-data/).**

# Recursos necessários
* Python 3 com a lib [*pyspark*](https://pypi.org/project/pyspark/)
* Java 8+
* [Jupyter Notebook](https://www.anaconda.com/distribution/)
* [Scala](https://www.scala-lang.org/download/) (2.12.11)
* [Spark 3](https://spark.apache.org/downloads.html)
* Uma IDE (Eclipse, Intelij...) ou o [sbt](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html) para rodar no terminal


# Como executar -> Linux
* Após todos os recursos instalados, para conseguir executar o spark dentro do jupiter notebook é necessário criar quatro variáveis de ambiente.
  ```
    export PYSPARK_PYTHON=python3
    export PYSPARK_DRIVER_PYTHON=jupyter
    export PYSPARK_DRIVER_PYTHON_OPTS='notebook --port=8899'
    export JAVA_HOME='/usr/lib/jvm/java-8-openjdk-amd64'
  ```

  
* Após a criação dessas variáveis, basta navegar até a pasta **Python Codes** e utilizar o comando ```pyspark``` no seu shell

* Para rodar em scala utilize a IDE ou então sbt ~run dentro da pasta do projeto


---
**Link para os datasets**  

[1] [mk-100](https://grouplens.org/datasets/movielens/)
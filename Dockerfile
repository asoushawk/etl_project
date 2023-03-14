# start by pulling the python image
FROM docker.io/ubuntu

# copy the requirements file into the image
#COPY ./requirements.txt /app/requirements.txt

# switch working directory
#WORKDIR /app

#VOLUME 

# install the dependencies and packages in the requirements file
#RUN pip install -r requirements.txt
# copy every content from the local file to the image
#COPY . /app

#RUN add-apt-repository ppa:webupd8team/java
#RUN apt-get update
##RUN apt-get install oracle-java8-installer
# configure the container to run in an executed manner
#ENTRYPOINT [ "bash" ]

#RUN bin/bash

#CMD ["/home/install_comands.sh" ]


#docker run -it -v $(pwd)/home:/home great_khayyam

#docker run -it -v $(pwd)/home:/home --network spark_project_network bin/bash
# Docker Orchestration for Copernicus Land Monitoring/In Situ

## Installation

1. Install [Docker](https://www.docker.com/).

2. Install [Docker Compose](https://docs.docker.com/compose/).


## Usage

### Development

In order to be able to edit source-code on your machine using your favorite editor, without having to do it inside a Docker container, you'll have to create a new user on your laptop with `uid=500` and use this user for development:

    $ useradd -u 500 zope-www
    $ usermod -a -G docker zope-www
    $ sudo su - zope-www


Now you need to edit and run de application with that zope-www user.


Start the application:

    $ docker-compose up

Within your favorite browser head to http://localhost:8080, add a Plone site, go to Admin -> Site Setup -> Addons and install the following add-ons:
* clms.downloadtool

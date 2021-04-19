FROM eeacms/plonesaas:5.2.2-2

RUN mkdir -p /plone/instance/src/clms.downloadtool/src
COPY . /plone/instance/src/clms.downloadtool/
RUN chown -R plone:plone /plone/instance/src/clms.downloadtool
COPY site.cfg /plone/instance/
RUN gosu plone buildout -c site.cfg

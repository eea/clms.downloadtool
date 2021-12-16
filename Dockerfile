FROM eeacms/clms-backend:1.0.49

RUN mkdir -p /plone/instance/src/clms.downloadtool/src
COPY . /plone/instance/src/clms.downloadtool/
RUN chown -R plone:plone /plone/instance/src/clms.downloadtool
COPY site.cfg /plone/instance/
RUN gosu plone buildout -c site.cfg

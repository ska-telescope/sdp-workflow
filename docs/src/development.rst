Workflow development
====================

The steps to develop and test an SDP workflow as follows:

- Clone the ska-sdp-science-pipelines repository from GitLab and create a new branch for
  your work.

- Create a directory for your workflow in ``src/workflows``:

  .. code-block::

    $ mkdir src/workflows/<my-workflow>
    $ cd src/workflows/<my-workflow>

- Write the workflow script (``<my-workflow>.py``). See the existing workflows
  for examples of how to do this. The ``test_realtime`` and ``test_batch``
  workflows are the best place to start.

- Create a ``requirements.txt`` file with your workflows's Python requirements,
  e.g.

  .. code-block::

    --index-url https://nexus.engageska-portugal.pt/repository/pypi/simple
    --extra-index-url https://pypi.org/simple
    ska-logging
    ska-sdp-workflow

- Create a ``Dockerfile`` for building the workflow image, e.g.

  .. code-block::

    FROM python:3.7

    COPY requirements.txt ./
    RUN pip install -r requirements.txt

    WORKDIR /app
    COPY <my-workflow>.py ./
    ENTRYPOINT ["python", "<my-workflow>.py"]

- Create a file called ``version.txt`` containing the semantic version number of
  the workflow.

- Create a ``Makefile`` containing

  .. code-block::

    NAME := workflow-<my-workflow>
    VERSION := $(shell cat version.txt)

    include ../../make/Makefile

- Build the workflow image:

  .. code-block::

    $ make build

  This will add it to your local Docker daemon where it can be used for testing
  with a local deployment of the SDP.

  For example with a local installation of ``minikube``
  
  .. code-block::
  
     $ eval $(minikube -p minikube docker-env)
     $ make build
     $ make tag_release
     
  This will point Docker towards the ``minikube`` Docker repository and will then build and
  tag the new workflow accordingly.

- Add your workflow to the GitLab CI file (``.gitlab-ci.yml``) in the root of the
  repository. This will enable the Docker image to be built and pushed to the
  EngageSKA repository when it is merged into the master branch.

- Add the workflow to the workflow definition file
  ``src/workflows/workflows.json``.

- Commit the changes to your branch and push to GitLab.

- You can then test the workflow by starting SDP with the processing
  controller workflows URL pointing to your branch in GitLab:

  .. code-block::

    $ helm repo add ska https://nexus.engageska-portugal.pt/repository/helm-chart
    $ helm install test ska/sdp \
    --set proccontrol.workflows.url=https://gitlab.com/ska-telescope/ska-sdp-science-pipelines/-/raw/<my-branch>/workflows.json

- Then create a processing block to run the workflow, either via the Tango
  interface, or by creating it directly in the config DB with ``sdpcfg``.

Receive Process and Port Configuration
======================================

Multiple Port Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The workflow library has the capability to configure multiple ports.
This allows to deploy a single receiver with multiple ports. In another word, this will allow a single receiver
to receive data for a single SPEAD stream coming from multiple processes.

Now, assuming each sender sends data for 1 channel and all baselines, then we'll want to have as many ports as
channels on the receiver side.  For cbf-receive, a single receiver process can receive on multiple ports already,
and this is configurable via ``reception.receiver_port_start`` and ``reception.num_ports``.

To make sense of multiple ports, the port map was required to be updated from a three-value list (ADR-10) to a
four-value list. The four value defines the increment of the port number.

For example, if we set ``reception.receiver_port_start = 9000`` and ``reception.num_ports = 3`` , ``count= 3``,
and ``max_channels=1`` then the resulting port_map would look like:

.. code-block::

    "port": [[0, 9000, 1, 0], [1, 9001, 1, 1], [2, 9002, 1, 2]]
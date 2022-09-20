# flyem-recon-qc
Quality control scripts used in EM reconstruction

## detect_multiple_soma_neurons.py
Script to detect bodies that have two or more soma located on them.
Requires this Python libraries to be installed:
* [neuclease](https://github.com/stuarteberg/neuclease)
* [pandas](https://pandas.pydata.org/)

## synapse_consistency_checks.py
Script to detect inconsistencies pertaining to synapses and their connections.
Requires this Python libraries to be installed:
* [neuclease](https://github.com/stuarteberg/neuclease)
* [pandas](https://pandas.pydata.org/)
* logging

## detect_stale_body_annotations.py
Script to detect stale body annotations, ei. bodies that do not exists anymore in the dataset.
Requires this Python libraries to be installed:
* [neuclease](https://github.com/stuarteberg/neuclease)
* [pandas](https://pandas.pydata.org/)
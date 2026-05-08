Model artifacts are runtime or offline-training outputs.

Do not commit `.pkl` files here unless the training dataset, feature schema, validation metrics, and rollout reason are documented with the artifact. The live worker should not silently persist synthetic fallback models into this directory.

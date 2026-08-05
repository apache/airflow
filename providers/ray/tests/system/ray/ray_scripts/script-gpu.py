# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import time

import ray
import torch

ray.init(address="auto")


@ray.remote(num_gpus=1)
def gpu_task() -> None:
    # Check if CUDA (GPU support) is available in PyTorch
    if torch.cuda.is_available():
        # Create a random tensor and move it to GPU
        data = torch.randn([1000, 1000]).cuda()
        print("Random data generated using torch.randn...")
        # Perform a simple computation (matrix multiplication) on the GPU
        torch.matmul(data, data.t())
        print("Matrix multiplication done with transpose of earlier matrix...")
        # Simulate some processing time
        print("sleeping using time.sleep()...")
        time.sleep(1)
        # Print a success message
        print("This task successfully performed a GPU-accelerated computation.")
    else:
        print("CUDA is not available. This task did not run on a GPU.")

    return


gpu_future = gpu_task.remote()

for _ in range(5):
    ray.get(gpu_future)
    time.sleep(3)

# Copyright 2021 Pinterest.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from pymemcache.client.murmur3 import murmur3_32


class RendezvousHash(object):
    """
        Implements the Highest Random Weight (HRW) hashing algorithm most
        commonly referred to as rendezvous hashing.

        Originally developed as part of python-clandestined.

        Copyright (c) 2014 Ernest W. Durbin III
    """
    def __init__(self, nodes=None, seed=0, hash_function=murmur3_32):
        """
        Constructor.
        """
        self.nodes = []
        self.seed = seed
        if nodes is not None:
            self.nodes = nodes
        self.hash_function = lambda x: hash_function(x, seed)

    def add_node(self, node):
        if node not in self.nodes:
            self.nodes.append(node)

    def remove_node(self, node):
        if node in self.nodes:
            self.nodes.remove(node)
        else:
            raise ValueError("No such node %s to remove" % (node))

    def get_node(self, key):
        high_score = -1
        winner = None

        for node in self.nodes:
            score = self.hash_function(
                "%s-%s" % (node, key))

            if score > high_score:
                (high_score, winner) = (score, node)
            elif score == high_score:
                (high_score, winner) = (score, max(str(node), str(winner)))

        return winner

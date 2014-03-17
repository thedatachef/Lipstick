;GraphBuilder = {
    
    MapReduce: function(data) {
        var self = this;
        self.jobId = ko.observable(data.jobId);
        self.stepType = ko.observable(data.stepType);
    },

    FieldSchema: function(data) {
        var self = this;
        self.alias = ko.observable(data.alias);
        self.type = ko.observable(data.type);
        self.schema = ko.observableArray(data.schema); // FIXME - need to recurse here
    },
    
    Location: function(data) {
        var self = this;
        self.line = ko.observable(data.line);
        self.filename = ko.observable(data.filename);
        self.macro = ko.observableArray(data.macro);
    },

    Node: function(data) {
        var self = this;

        self.uid = ko.observable(data.uid);          
        self.successors = ko.observableArray(data.successors);
        self.predecessors = ko.observableArray(data.predecessors);

        self.schema = ko.observableArray();
        ko.utils.arrayForEach(data.schema, function(fieldSchema) {
            self.schema.push(new GraphBuilder.FieldSchema(fieldSchema));
        });

        self.operator = ko.observable(data.operator);
        self.alias = ko.observable(data.alias);
        self.location = ko.observable(new GraphBuilder.Location(data.location));
        self.mapReduce = ko.observable(new GraphBuilder.MapReduce(data.mapReduce));
        self.schemaString = ko.observable(data.schemaString);
        self.data = data;        
        
        self.additionalInfo = ko.computed(function () {
            if (self.location().macro().length > 0) {
                return "MACRO: " + self.location().macro()[0];
            }

            if (self.operator() === "LOLimit") {
                return self.data.rowLimit;
            }

            if (self.operator() === "LOJoin") {
                return data.join.type + ", " + self.data.join.strategy;
            }
            return "";
        });

        self.getColSpan = ko.computed(function () {
            var colspan = null;
                                          
            if (self.operator() === "LOJoin" || self.operator() === "LOCogroup") {
                var join;
                if (self.operator() === "LOJoin") {
                    join = self.data.join;
                } else {
                    join = self.data.group;
                }
                colspan = join.expression.length;
            }
            return colspan ? colspan : "2";
        });

        self.operation = ko.computed(function() {
            var info = self.additionalInfo();
            if (info) {
                info = " (" + info + ")";
            }
            var op = null;
            if (self.operator() === "LOCogroup" && self.data.group.expression.length < 2) {
                op = "GROUP";
            } else {
                op = self.operator().substring(2).toUpperCase();
            }                                         
            return op+info;   
        });

        self.miscRows = ko.computed(function() {
          var expression = null;
            var result = [];
            if (self.operator() === "LOFilter") {
                expression = self.data.expression;
            }
            if (self.operator() === "LOSplitOutput") {
                expression = self.data.expression;
            }
            if (expression) {
                result.push({rowType: 'expression-row', colspan: self.getColSpan(), text: expression});
            }
            var storageLocation = null;
            var storageFunction = null;
            if (self.operator() === "LOStore") {
                storageLocation = self.data.storageLocation;
                storageFunction = self.data.storageFunction;
            } else if (self.operator() === "LOLoad") {
                storageLocation = self.data.storageLocation;
                storageFunction = self.data.storageFunction;
            }
            if (storageLocation) {
                result.push({rowType: 'uri-row', colspan: self.getColSpan(), text: storageLocation});
                result.push({rowType: 'function-row', colspan: self.getColSpan(), text: storageFunction});
            }
            return result;
        });
        
        self.joinColumns = ko.computed(function () {
            if (self.operator() === "LOJoin" || self.operator() === "LOCogroup") {
                var join;
                if (self.operator() === "LOJoin") {
                    join = self.data.join;
                } else {
                    join = self.data.group;
                }

                var expressions = [];
                for (var key in join.expression) {
                    expressions.push(join.expression[key].fields);
                }
                var result = [];
                if (expressions.length > 1) {
                    var tr = {rowType: 'join-expression-header'}
                    var cols = [];
                    for (var key in join.expression) {
                        cols.push({field: (key ? key : "null")});
                    }
                    tr.cols = cols;
                    result.push(tr);
                }
                for (var i = 0; i < expressions[0].length; i++) {
                    var tr = {rowType: 'join-expression-body'};
                    var cols = [];
                    for (var j = 0; j < expressions.length; j++) {
                        cols.push({field: expressions[j][i]});
                    }
                    tr.cols = cols;
                    result.push(tr);
                }
                return result;
            }
            return [];                                               
        });
        
        self.schemaEqualsPredecessor = ko.computed(function () {
            if (self.schemaString()) {
                var operString = self.schemaString().substring(1, self.schemaString().length - 1);
                for (var i = 0; i < self.predecessors().length; i++) {
                    var pred = self.predecessors()[i];
                    if (pred instanceof GraphBuilder.Node && pred.schemaString()) {
                        var predString = pred.schemaString().substring(1, pred.schemaString().length - 1);
                        if (predString != operString) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                return true;
            }
            return false;
        });
        
        self.displaySchema = ko.computed(function () {                                             
            if (self.location().line() != null                
                && !self.schemaEqualsPredecessor()
                && self.operator() != "LOSplit"
                && self.operator() != "LOFilter"
                && self.operator() != "LODistinct"
                && self.operator() != "LOLimit"
                && self.operator() != "LOJoin"
                && self.operator() != "LOCogroup") {
                return true;
            }
            return false;
        });

        self.schemaRows = ko.computed(function () {
            var result = [];                                          
            if (self.displaySchema()) {
                if (self.schema()) {
                    for (var i = 0; i < self.schema().length; i++) {
                        var fieldSchema = self.schema()[i];
                        var tr = {
                            alias: (fieldSchema.alias() ? fieldSchema.alias() : "?"),
                            type: fieldSchema.type()
                        }; 
                        result.push(tr);
                    }
                }
            }
            return result;
        });
        
        self.opType = ko.computed(function () {
            if (self.mapReduce()) {
                var stepType = self.mapReduce().stepType();
                if (stepType === "MAPPER") {
                    return 'map';
                }
                if (stepType === "REDUCER") {
                    return 'reduce';
                }
            }
            return 'none';
        });

        self.label = ko.computed(function() {
            return "<div class=\"node-html\" data-bind=\"template: {name: \'node-template\', data: nodes['"+self.uid()+"']}\"></div>";
        });
     },
     
    Edge: function(data) {
        var self = this;
        self.id = ko.observable(data.id);       // "u-v"
        self.u = ko.observable(data.u);         // From node id
        self.v = ko.observable(data.v);         // To node id

        self.scope = ko.observable('');
        self.recordCount = ko.observable(''); // Edge value
                  
        self.label = ko.computed(function() {
            return self.recordCount();                                      
        });
    },
    
    Cluster: function(clusterId) {
        var self = this;
        self.id = ko.observable(clusterId);
        self.running = ko.observable(false);
        self.runType = ko.observable('');
        self.css = ko.computed(function() {
          if (self.running()) {
              return 'cluster running '+self.runType();
          } else {
              return 'cluster';
          }   
        });
    },
    
    ViewModel: function () {
        var self = this;         
         
        self.graph = new graphlib.CDigraph();
        self.subGraphs = {};
        self.nodes = {};
        self.edges = {};
        self.clusters = {};
        
        self.addNode = function(node) {             
            if (node.mapReduce() && node.mapReduce().jobId()) {
                var jid = node.mapReduce().jobId();
                if (!self.subGraphs.hasOwnProperty(jid)) {
                    var subGraph = new dagreD3.Digraph();
                    self.subGraphs[jid] = subGraph;
                    self.clusters[jid] = new GraphBuilder.Cluster(jid);
                }
            
                var subGraph = self.subGraphs[jid];
                if (!subGraph.hasNode(node.uid())) {
                    subGraph.addNode(node.uid(), {label: node.label()});
                }
            }
                                  
            if (!self.graph.hasNode(node.uid())) {
                self.graph.addNode(node.uid(), {label: node.label()});               
            } else {
                self.graph.node(node.uid(), {label: node.label()});
            }
            self.nodes[node.uid()] = node;
        };                               
         
        self.addEdge = function(edge) {
            //
            // Boundary edge?
            //            
            var source = self.node(edge.u());
            var target = self.node(edge.v());
            if (source.mapReduce().jobId() != target.mapReduce().jobId() && source.operator() !== 'LOSplit') {
                edge.scope(source.mapReduce().jobId()); // out edge
                edge.recordCount('0');
            }

            if (!self.graph.hasEdge(edge.id())) {
                self.graph.addEdge(edge.id(), edge.u(), edge.v(), {label: edge.label()});
             }  
            self.edges[edge.id()] = edge;
        };

        //
        // Visit all subgraph nodes
        //
        self.getGraph = function() {
            for (var key in self.subGraphs) {
                var subGraph = self.subGraphs[key];
                if (!self.graph.hasNode(key)) {
                    self.graph.addNode(key, {label: "cluster", data: self.clusters[key]});
                }
                subGraph.eachNode(function(u, value) {
                    // Set cluster id
                    self.graph.parent(u, key);

                    // Append actual predecessor nodes
                    var node = self.node(u);
                    node.predecessors(self.predecessors(u));
                    self.node(u, node);
                });
            }
            return self.graph;
        };
         
        self.edge = function(edgeId, edge) {
            if (edge) {
                self.graph.edge(edgeId, {label: edge.label()});
            } else {
                return self.edges[edgeId];
            }
        };
 
        self.node = function(nodeId, node) {
            if (node) {
                return self.graph.node(nodeId, {label: node.label()});
            } else {
                return self.nodes[nodeId];
            }
        };         

        self.predecessors = function(nodeId) {
            var preds = self.node(nodeId).predecessors();
            var result = [];
            ko.utils.arrayForEach(preds, function(pred) {
                result.push(self.node(pred));
            });
            return result;
        };

        self.update = function(runStats) {
            console.log(runStats.jobStatusMap);                
            for (var key in runStats.jobStatusMap) {
                var job = runStats.jobStatusMap[key];
                var clusterId = job['scope'];
                var running = (!job['isComplete'] && !job['isSuccessful']);                
                // Update clusters, nodes, edges here with runtime data

                self.clusters[clusterId].running(running);
                
                if (job['mapProgress'] < 1.0) {
                    self.clusters[clusterId].runType('running-map');    
                } else if (job['totalReducers'] > 0) {
                    self.clusters[clusterId].runType('running-reduce');    
                }

                if (job.counters.hasOwnProperty('Map-Reduce Framework')) {

                    if (job.counters.hasOwnProperty('MultiStoreCounters')) {
                        _.each(jobStats.counters.MultiStoreCounters.counters, function(count, counter) {
                                   GraphView.displayRecordCount(scopeId+'-out', count, counter);
                        });
                    }
                    else {
                        count_out = (job.counters['Map-Reduce Framework'].counters.hasOwnProperty('Reduce output records')) ? job.counters['Map-Reduce Framework'].counters['Reduce output records'] : '';
                        if (job.totalReducers === 0) {
                             count_out = jobStats['counters']['Map-Reduce Framework']['counters']['Map output records'];
                        }
                        for (var edgeId in self.edges) {
                            var edge = self.edges[edgeId];
                            if (edge.scope() === clusterId) {                            
                                edge.recordCount(count_out);
                            }
                        }
                    }
                }                
            }
        };
     },

    addNodes: function(data, nodes, vm) {
        for (var i = 0; i < nodes.length; i++) {
            var node = data[nodes[i]];
            vm.addNode(new GraphBuilder.Node(node));            
        }
    },

    addEdges: function(data, node, preds, vm) {
        for (var i = 0; i < preds.length; i++) {
            var pred = data[preds[i]];
            vm.addEdge(new GraphBuilder.Edge({id: pred.uid+"->"+node.uid, u: pred.uid, v: node.uid}));
        }
    },

    populateViewModel: function (data, vm, callback) {
        var nodes = Object.keys(data);
        GraphBuilder.addNodes(data, nodes, vm);
        for (var i = 0; i < nodes.length; i++) {
            var node = data[nodes[i]];
            if (node.predecessors && node.predecessors.length > 0) {
                GraphBuilder.addEdges(data, node, node.predecessors, vm);
            }
        }
        callback(vm.getGraph());
    },

    buildGraph: function(data, callback) {
        var viewModel = new GraphBuilder.ViewModel();
        GraphBuilder.populateViewModel(data, viewModel, function (graph) {
          callback(graph, viewModel);
        });
    }
};

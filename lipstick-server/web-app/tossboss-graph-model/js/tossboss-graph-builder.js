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

        self.clicked = function (data, event, thing) {
            $(event.currentTarget).trigger('clickLogicalOperator.tossboss-graph-view', [data.uid()]);
        };
        
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
                    var tr = {rowType: 'join-expression-header'};
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

        self.storageLocation = ko.observable('');
        self.intermediate = ko.observable(false);
        self.boundaryEdge = ko.observable(false);
        self.inEdge = ko.observable(false);
        self.outEdge = ko.observable(false);        
        self.scope = ko.observable('');
        
        self.edgeCss = ko.computed(function () {
            return self.scope() + '-out';
        });
        
        self.recordCount = ko.observable('').extend({notify: 'always'}); // Edge value
        self.formattedRecordCount = ko.computed(function() {
            return GraphView.addCommas(self.recordCount());
        }).extend({notify: 'always'});

        
        //
        // When the recordCount is updated we need to expand
        // the text box
        //
        self.formattedRecordCount.subscribe(function (newValue) {            
            var width;
            var edgeLabel = d3.select('.foreign-html.edge-'+self.id().replace('>',''));
            
            var count = edgeLabel.select('div.edge-record-count');
            var icon = edgeLabel.select('i');

            if (count.node() && newValue) {
                var oldWidth = count.node().clientWidth+icon.node().clientWidth;
                var oldX = parseFloat(edgeLabel.attr('x') ? edgeLabel.attr('x') : 0);
                count.html(newValue);
                var newWidth = count.node().clientWidth+icon.node().clientWidth;
                var newX = (oldWidth - newWidth)/2.0 + oldX;

                edgeLabel.attr('width', newWidth);
                edgeLabel.attr('x', newX);
            }
         });

        self.displayExamples = function (data, event) {
            $(event.currentTarget).trigger('clickEdge.tossboss-graph-view', [self.u(), '', self.scope(), '']);
        };
        
        self.label = ko.computed(function() {
            return "<div class=\"edge-html\" data-bind=\"template: {name: \'edge-template\', data: edges['"+self.id()+"']}\"></div>";
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

        self.s3StorageFunctions = ['PigStorage','AegisthusBagLoader','AegisthusMapLoader','PartitionedLoader','PartitionedStorer'];
        self.dumpStorageFunctions = ['TFileStorage','InnerStorage'];
        
        self.clickedCluster = function (data, event) {
            $(event.currentTarget).trigger('clickMRJob.tossboss-graph-view', [event.currentTarget.id]);
        };

        self.mouseEnterCluster = function (data, event) {
            $(event.currentTarget).trigger('mouseEnterMRJob.tossboss-graph-view', [event.currentTarget.id]);
        };

        self.mouseLeaveCluster = function (data, event) {
            $(event.currentTarget).trigger('mouseLeaveMRJob.tossboss-graph-view', [event.currentTarget.id]);
        };
        
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
                    subGraph.addNode(node.uid(), {label: node.label(), type: 'node', id: node.uid()});
                }
            }
                                  
            if (!self.graph.hasNode(node.uid())) {
                self.graph.addNode(node.uid(), {label: node.label(), type: 'node', id: node.uid()});               
            } else {
                self.graph.node(node.uid(), {label: node.label(), type: 'node', id: node.uid()});
            }
            self.nodes[node.uid()] = node;
        };                               
        
        self.addEdge = function(edge) {
            //
            // Boundary edge?
            //            
            var source = self.node(edge.u());
            var target = self.node(edge.v());

            // In edge
            if (source.predecessors().length == 0) {
                edge.inEdge(true);
                edge.recordCount(0); // so that renderer works. FIXME - renderer edgeLabel size needs to be dynamic
                if (source.data.hasOwnProperty('storageLocation')) {
                    var storageLocation = source.data.storageLocation.slice(-40);
                    if (_.contains(self.s3StorageFunctions, source.data.storageFunction)) {
                        storageLocation = storageLocation.split('/').reverse()[0].slice(-40);
                    }
                    edge.storageLocation(storageLocation);
                }
            }

            // Out edge
            if (target.successors().length == 0) {
                edge.outEdge(true);
                edge.recordCount(0); 
                if (target.data.hasOwnProperty('storageLocation')) {
                    var storageLocation = target.data.storageLocation.slice(-40);
                    if (_.contains(self.s3StorageFunctions, target.data.storageFunction)) {
                        storageLocation = storageLocation.split('/').reverse()[0].slice(-40);
                        edge.storageLocation(storageLocation);
                    }
                    if (_.contains(self.dumpStorageFunctions, target.data.storageFunction)) {
                        edge.intermediate(true);
                    }
                }
            }

            // Boundary edge
            if (source.mapReduce().jobId() != target.mapReduce().jobId() && source.operator() !== 'LOSplit') {
                edge.boundaryEdge(true);
                edge.recordCount(0);
            }

            if (!self.graph.hasEdge(edge.id())) {
                self.graph.addEdge(edge.id(), edge.u(), edge.v(), {label: edge.label(), type: 'edge', id: edge.id()});
            }
            edge.scope(source.mapReduce().jobId());
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
                self.graph.edge(edgeId, {label: edge.label(), type: 'edge', id: edgeId});
            } else {
                return self.edges[edgeId];
            }
        };
 
        self.node = function(nodeId, node) {
            if (node) {
                return self.graph.node(nodeId, {label: node.label(), type: 'node', id: nodeId});
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
            for (var key in runStats.jobStatusMap) {
                var job = runStats.jobStatusMap[key];
                var clusterId = job['scope'];
                var running = (!job['isComplete'] && !job['isSuccessful']);                
                // Update clusters, nodes, edges here with runtime data

                var cluster = self.clusters[clusterId];
                if (cluster) {
                    cluster.running(running);
                    if (job['mapProgress'] < 1.0) {
                        cluster.runType('running-map');    
                    } else if (job['totalReducers'] > 0) {
                        cluster.runType('running-reduce');    
                    }
                }                                

                if (job.counters.hasOwnProperty('Map-Reduce Framework')) {

                    if (job.counters.hasOwnProperty('MultiInputCounters')) {
                        _.each(job.counters.MultiInputCounters.counters, function(count, counter) {
                            var counterStorageLocation = counter.split('_').slice(2).join('_');
                            for (var edgeId in self.edges) {                            
                                var edge = self.edges[edgeId];
                                if (edge.storageLocation() == counterStorageLocation && edge.inEdge() && edge.scope() === clusterId) {
                                    edge.recordCount(count);
                                }
                            }
                        });
                    }
                    else {
                        count_in = job.counters['Map-Reduce Framework'].counters['Map input records'];
                        for (var edgeId in self.edges) {                            
                            var edge = self.edges[edgeId];                            
                            if (edge.inEdge() && edge.scope() === clusterId) {
                                edge.recordCount(count_in);
                            }
                        }
                    }
                    
                    if (job.counters.hasOwnProperty('MultiStoreCounters')) {
                        _.each(job.counters.MultiStoreCounters.counters, function(count, counter) {
                            var counterStorageLocation = counter.split('_').slice(2).join('_');
                            for (var edgeId in self.edges) {                            
                                var edge = self.edges[edgeId];
                                if (edge.storageLocation() == counterStorageLocation && edge.outEdge() && edge.scope() === clusterId) {
                                    edge.recordCount(count);
                                }
                            }
                        });
                    }
                    else {
                        // Update out edges
                        count_out = (job.counters['Map-Reduce Framework'].counters.hasOwnProperty('Reduce output records')) ? job.counters['Map-Reduce Framework'].counters['Reduce output records'] : '';
                        if (job.totalReducers === 0) {
                             count_out = job['counters']['Map-Reduce Framework']['counters']['Map output records'];
                        }
                        for (var edgeId in self.edges) {                            
                            var edge = self.edges[edgeId];                            
                            if ((edge.boundaryEdge() || edge.outEdge()) && edge.scope() === clusterId) {
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

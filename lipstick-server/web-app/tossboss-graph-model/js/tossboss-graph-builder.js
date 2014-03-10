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
        
        // FIXME: remove this; should use all observables
        self.data = data;    
    
        // Need to have a function to -read- all values
        self.node = ko.computed(function() {
          var result = {
              uid: self.uid(),
              successors: self.successors(),
              predecessors: self.predecessors(),
              schema: self.schema(),
              operator: self.operator(),
              alias: self.alias(),
              location: self.location(),
              mapReduce: self.mapReduce(),
              schemaString: self.schemaString(),
              data: data
          };                    
          return result;
         });    
     },
     
     Edge: function(data) {
         var self = this;
         self.id = ko.observable(data.id);       // "u-v"
         self.u = ko.observable(data.u);         // From node id
         self.v = ko.observable(data.v);         // To node id
         self.value = ko.observable(data.value); // Edge value

         self.edge = ko.computed(function() {
           var result = {
               id: self.id(),
               u: self.u(),
               v: self.v()
           };
           return result;
         });
     },

     ViewModel: function () {
         var self = this;         
         self.nodes = ko.observableArray([]);
         self.edges = ko.observableArray([]);         

         self.addNode = function(node) {
             self.nodes.push(node);
         };
         
         self.addEdge = function(edge) {
             self.edges.push(edge);
         };

         self.editNode = function (nodeId) {
             var toEdit = ko.utils.arrayFirst(self.nodes(), function(node) {
               return (node.id() == nodeId);
             });
            
            console.log(toEdit.node());   
         };
     },
    
    populateViewModel: function (data, vm, callback) {
        for (var key in data) {
            if (data.hasOwnProperty(key)) {
                var node = data[key];
                vm.addNode(new GraphBuilder.Node(node));
                if (node.predecessors && node.predecessors.length > 0) {
                    for (var i = 0; i < node.predecessors.length; i++) {
                        var pred = data[node.predecessors[i]];
                        vm.addNode(new GraphBuilder.Node(pred)); // Trying to add the node too many times; ok for now
                        vm.addEdge(new GraphBuilder.Edge({id: pred.uid+"-"+node.uid, u: pred.uid, v: node.uid}));
                    }
                }
            }
        }
        callback();
    },
        
    updateNodes: function(nodes, graph) {
        ko.utils.arrayForEach(nodes, function(node) {
          // Only add the node if it does not already exist
          if (!graph.hasNode(node.uid())) {
              graph.addNode(node.uid(), node.node());               
          }
        });             
    },

    updateEdges: function(edges, graph) {
        ko.utils.arrayForEach(edges, function(edge) {
          if (!graph.hasEdge(edge.id())) {
              graph.addEdge(edge.id(), edge.u(), edge.v());
          }
        });
    },

    buildGraph: function(data, callback) {
        var viewModel = new GraphBuilder.ViewModel();
        var graph = new dagreD3.Digraph();
        var nodeSubscriptions = [];
        var edgeSubscriptions = [];
          
        viewModel.nodes.subscribe(function (nodes) {
            GraphBuilder.updateNodes(nodes, graph);
            ko.utils.arrayForEach(nodeSubscriptions, function(sub) { sub.dispose(); } );
            ko.utils.arrayForEach(nodes, function(item) {
              nodeSubscriptions.push(item.node.subscribe(function() {
                GraphBuilder.updateNodes(nodes);
              }));
            });
        });
          
        viewModel.edges.subscribe(function (edges) {
            GraphBuilder.updateEdges(edges, graph);
           
            ko.utils.arrayForEach(edgeSubscriptions, function(sub) { sub.dispose(); } );
            ko.utils.arrayForEach(edges, function(item) {
              edgeSubscriptions.push(item.edge.subscribe(function() {
                GraphBuilder.updateEdges(edges);
              }));
            });
        });
        GraphBuilder.populateViewModel(data, viewModel, function () {
          callback(graph, viewModel);
        });
    }
};

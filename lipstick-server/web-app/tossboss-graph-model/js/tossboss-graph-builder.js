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

        self.operationRow = ko.computed(function () {
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
          var result = "<tr class=\"node-row operation-row {{opType}}\"><td colspan=\"{{colspan}}\">"+op+info+"</td></tr>";
          return result;
        });

        self.miscRows = ko.computed(function () {
          var expression = null;
          var result = [];
          if (self.operator() === "LOFilter") {
            expression = self.data.expression;
          }
          if (self.operator() === "LOSplitOutput") {
            expression = self.data.expression;
          }
          if (expression) {
            result.push("<tr class=\"node-row misc-row expression-row\"><td colspan=\"{{colspan}}\">"+expression+"</td></tr>");
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
            result.push("<tr class=\"node-row misc-row uri-row\"><td colspan=\"{{colspan}}\">"+storageLocation+"</td></tr>");
            result.push("<tr class=\"node-row misc-row function-row\"><td colspan=\"{{colspan}}\">"+storageFunction+"</td></tr>");
          }
          return result;
        });

        self.joinExpressions = ko.computed(function () {
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
                var tr = "<tr class=\"node-row join-expression join-expression-header\">";
                for (var key in join.expression) {
                    tr += "<td>";
                    tr += (key ? key : "null");
                    tr += "</td>";
                }
                tr += "</tr>";
                result.push(tr);
            }
            for (var i = 0; i < expressions[0].length; i++) {
                var tr = "<tr class=\"node-row join-expression join-expression-body\">";
                for (var j = 0; j < expressions.length; j++) {
                    tr += "<td>";
                    tr += expressions[j][i];
                    tr += "</td>";
                }
                tr += "</tr>";
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
                if (pred.schemaString) {
                    var predString = pred.schemaString.substring(1, pred.schemaString.length - 1);
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

                    var tr = "<tr class=\"node-row field-schema-row\">";                    
                    tr += "<td>"+(fieldSchema.alias() ? fieldSchema.alias() : "?")+"</td>";
                    tr += "<td>"+fieldSchema.type()+"</td>";
                    tr += "</tr>";
                    result.push(tr);
                }
            }
          }
          return result;
        });
        
        self.displayTemplate = ko.computed(function () {
          var resultRows = [];
          resultRows.push('<div class="node-html">');                                               
          resultRows.push('<table><tbody>');                                               

          // Operation row                                               
          resultRows.push(self.operationRow());
                                               
          // Misc rows
          for (var i = 0; i < self.miscRows().length; i++) {                                               
              resultRows.push(self.miscRows()[i]);
          }
                                               
          // Alias row
          if (self.alias() && self.operator() != "LOSplit") {
            resultRows.push("<tr class=\"node-row alias-row\"><td colspan=\"{{colspan}}\">"+self.alias()+"</td></tr>");
          }
          
          // Join expression
          for (var i = 0; i < self.joinExpressions().length; i++) {
              resultRows.push(self.joinExpressions()[i]);
          }                                     
                                               
          // Schema rows
          for (var i = 0; i < self.schemaRows().length; i++) {
              resultRows.push(self.schemaRows()[i]);
          }
                                               
          resultRows.push('</tbody></table>');                                               
          resultRows.push('</div>');                                               
          return Handlebars.compile(resultRows.join(''));
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
              data: data,


              opType: self.opType(),
              colspan: self.getColSpan()
          };
          var template = self.displayTemplate();                                    
          result.label = template(result);                           
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
               v: self.v(),
               value: self.value()
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

    // FIXME - yes, obviously this is bad code; works for now
    populateViewModel: function (data, vm, callback) {
        for (var key in data) {
            if (data.hasOwnProperty(key)) {
                var node = data[key];               
                vm.addNode(new GraphBuilder.Node(node));
                if (node.predecessors && node.predecessors.length > 0) {
                    var predecessors = [];
                    for (var i = 0; i < node.predecessors.length; i++) {
                        var pred = data[node.predecessors[i]];
                        predecessors.push(pred);
                        vm.addNode(new GraphBuilder.Node(pred)); // Trying to add the node too many times; ok for now
                        vm.addEdge(new GraphBuilder.Edge({id: pred.uid+"-"+node.uid, u: pred.uid, v: node.uid}));
                    }                    
                    node.predecessors = predecessors;
                    vm.addNode(new GraphBuilder.Node(node));
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
          } else {
              graph.node(node.uid(), node.node());
          }
        });             
    },

    updateEdges: function(edges, graph) {
        ko.utils.arrayForEach(edges, function(edge) {
          if (!graph.hasEdge(edge.id())) {
              graph.addEdge(edge.id(), edge.u(), edge.v(), edge.value());
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
          callback(graph);
        });
    }
};

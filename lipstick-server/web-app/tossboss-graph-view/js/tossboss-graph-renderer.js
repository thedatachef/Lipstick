;GraphRenderer = {
    
    options: {
        bgCluster: "#E9E9E9",
        bgAlias: "#424242",
        bgExpression: "#BCBCBC",
        bgMapTask: "#3299BB",
        bgRedTask: "#FF9900",
        bgUnkTask: "#BF0A0D",
        bgWhite: "#FFFFFF"
    },        

    getJobColor: function(node) {
        if (node.mapReduce) {
            var stepType = node.mapReduce.stepType();
            if (stepType === "MAPPER") {
                return GraphRenderer.options.bgMapTask;
            }
            if (stepType === "REDUCER") {
                return GraphRenderer.options.bgRedTask;
            }
        }
        return GraphRenderer.options.bgUnkTask;
    },

    schemaEqualsPredecessor: function(node, graph) {
        if (node.schemaString) {
            var operString = node.schemaString.substring(1, node.schemaString.length - 1);
            var preds = graph.predecessors(node.uid);
            for (var i = 0; i < preds.length; i++) {                
                var pred = graph.node(preds[i]);
                if (pred.schemaString) {
                    var predString = pred.schemaString().substring(1, pred.schemaString().length - 1);
                    // Ouch, just do string comparison for now; no deep compare
                    return (predString === operString);
                } 
            }
            return true;
        }
        return false;
    },

    displaySchema: function(node) {
        if (node.location.line != null
            && !GraphRenderer.schemaEqualsPredecessor(node)
            && !node.operator.equalsIgnoreCase("LOSplit")
            && !node.operator.equalsIgnoreCase("LOFilter")
            && !node.operator.equalsIgnoreCase("LODistinct")
            && !node.operator.equalsIgnoreCase("LOLimit")
            && !node.operator.equalsIgnoreCase("LOJoin")
            && !node.operator.equalsIgnoreCase("LOCogroup")) {
            return true;
        }
        return false;
    },
    
    additionalInfo: function(node) {
        if (node.location.macro().length > 0) {
            return "MACRO: " + node.location().macro()[0];
        }
        if (node.operator === "LOLimit") {
            return node.data.rowLimit;
        }
        if (node.operator === "LOJoin") {
            return node.data.join.type + ", " + node.data.join.strategy;
        }
        return "";
    },
    
    genOperationRow: function(node) {
        var info = GraphRenderer.additionalInfo(node);
        if (info) {
            info = " (" + info + ")";
        }
        var op = null;
        if (node.operator === "LOCogroup" && node.data.group.expression.length < 2) {
            op = "GROUP";
        } else {
            op = node.operator.substring(2).toUpperCase();
        }
        var result = {
            text: op + info,
            color: GraphRenderer.getJobColor(node), 
            textcolor: '#000000'
        };
        return result;
    },
    
    genMiscRows: function(node) {
        var expression = null;
        var result = [];
        if (node.operator === "LOFilter") {
            expression = node.data.expression;
        }
        if (node.operator === "LOSplitOutput") {
            expression = node.data.expression;
        }
        if (expression) {
            result.push({text: expression, color: GraphRenderer.options.bgExpression, textcolor: '#000000'});
        }
        var storageLocation = null;
        var storageFunction = null;
        if (node.operator === "LOStore") {
            storageLocation = node.data.storageLocation;
            storageFunction = node.data.storageFunction;
        } else if (node.operator === "LOLoad") {
            storageLocation = node.data.storageLocation;
            storageFunction = node.data.storageFunction;
        }
        if (storageLocation) {
            result.push({text: storageLocation, color: GraphRenderer.options.bgExpression, textcolor: '#000000'});
            result.push({text: storageFunction, color: GraphRenderer.options.bgExpression, textcolor: '#000000'});
        }
        return result;
    },

    genJoinExpressions: function(node) {
        if (node.operator === "LOJoin" || node.operator === "LOCogroup") {
            var join = null;
            if (node.operator === "LOJoin") {
                join = node.data.join;
            } else {
                join = node.data.group;
            }

            var expressions = [];
            for (var key in join.expression) {
                expressions.push(join.expression[key].fields);
            }
            
            var result = [];
            // if (expressions.length > 1) {                
            //             for (Entry<String, JoinExpression> entry : expressions) {
            //         html.td().bgcolor(BG_EXPRESSION).text(entry.getKey() == null ? "null" : entry.getKey()).end();
            //     }
            //     html.end();
            // }
            // for (int i = 0; i < exp.get(0).size(); i++) {
            //     html.tr();
            //     for (int j = 0; j < exp.size(); j++) {
            //         html.td().bgcolor(BG_WHITE).text(exp.get(j).get(i)).end();
            //     }
            //     html.end();
            // }
            // html.end();
        }
    },

    nodeViewData: function(node, h) {
        var data = [];
        
        // Operator name
        var operationRow = GraphRenderer.genOperationRow(node);
        
        data.push(operationRow);
        
        var miscRows = GraphRenderer.genMiscRows(node);
        for (var i = 0; i < miscRows.length; i++) {
            data.push(miscRows[i]);            
        }
        
        if (node.alias && node.operator != "LOSplit") {
            data.push({text: node.alias, color: GraphRenderer.options.bgAlias, textcolor: '#FFFFFF'});
        }

        GraphRenderer.genJoinExpressions(node);
        
        for (var i = 0; i < data.length; i++) {
            var row = data[i];
            row.x = 0;
            row.y = -h + i*h;
            row.height = h;
        }
        return data;
    },

    addNode: function(node, root, marginX, marginY) {
        var rectSvg = root.append('rect');
        var nodeSvg = root.append('g');
        var rectH = 30;
        
        var data = GraphRenderer.nodeViewData(node, rectH);
        
        var labelGroup = nodeSvg.selectAll("g")
                           .data(data).enter().append("g");
        var rect = labelGroup.append("rect");
        var text = labelGroup.append("text");

        // Layout the text labels and get max label width 
        var maxWidth = 0;
        text
          .text(function(data) { return data.text; })
          .attr('text-anchor', 'end')
          .attr('font-family', 'Times,serif')
          .attr('font-size', '12.00')
          .attr('fill', function(data) { return data.textcolor; })
          .attr('y', function(data) { return data.y + data.height/2; })
          .each(function(data) { 
            var w = d3.select(this).node().getBBox().width; 
            if (w > maxWidth) { maxWidth = w; }
          });
        
        rect
          .attr('x', -(maxWidth/2+marginX) )
          .attr('y', function(data) { return data.y; } )
          .attr('width', maxWidth+2*marginX)
          .attr('height', rectH)
          .attr('fill', function(data) { return data.color; });
        
        // Align text in the center
        text.attr('transform', function(data) {
          var w = d3.select(this).node().getComputedTextLength();
          return 'translate('+w/2+',0)';
        });

        var bbox = root.node().getBBox();
        rectSvg
          .attr('x', -(bbox.width / 2))
          .attr('y', -rectH)
          .attr('width', bbox.width)
          .attr('height', bbox.height);        
    },
                    
    isComposite: function (graph, u) {
        return 'children' in graph && graph.children(u).length;
    },

    renderGraph: function (graph, callback) {        
        var renderer = new dagreD3.Renderer();
        var oldDrawNodes = renderer.drawNodes();        
        renderer.drawNodes(function(graph, root) {
          var nodes = graph.nodes().filter(function(u) { return !GraphRenderer.isComposite(graph, u); });
          
          var svgNodes = root
              .selectAll('g.node')
              .classed('enter', false)
              .data(nodes, function(u) { return u; });
          
          svgNodes.selectAll('*').remove();
          
          svgNodes
              .enter()
              .append('g')
              .style('opacity', 0)
              .attr('class', 'node enter');
          
          svgNodes.each(function(u) { GraphRenderer.addNode(graph.node(u), d3.select(this), 10, 10); });          
          svgNodes.attr('id', function(u) { return u; });
                               
          return svgNodes;
        });
        
        var svg = d3.select('#pig-graph').append('svg').append('g');
        renderer.run(graph, svg);
        var bbox = svg.node().getBBox();        
        var viewHeight = bbox.height+"pt";
        var viewWidth = bbox.width+"pt";
        var viewBox = "0.00 0.00 "+bbox.width+" "+bbox.height;
        var result = "<svg height=\""+viewHeight+"\" width=\""+viewWidth+"\" viewBox=\""+viewBox+"\">"+svg.html()+"</svg>";
        callback(result);
    }
};

const jsonUrl = "top500.json";
function nodeLabel(node) {
  return node.title + "-" + node.artist;
}

function getGraphDataSets() {
  const loadTop500 = function(Graph) {
    const tagReducer = (tagSummary, currentTag, index) =>
      (index === 1 ? tagSummary[0] : tagSummary) + ", " + currentTag[0];

    qwest.get(jsonUrl).then((_, data) => {
      data.nodes.forEach(function(node) {
        node.name = `${node.title ? node.title + " - " : ""}${node.artist ||
          node.id}`;
        node.pagerank = Math.pow(node.pagerank * 3.14, 2);
        node.desc = node.tags.slice(0, 3);
        if (node.desc.length > 0) {
          node.desc = node.desc.reduce(tagReducer);
        }
      });

      data.links.forEach(function(link) {
        link.source = link.src;
        link.target = link.dst;
        link.similar = link.similar * 10;
        link.weight = link.weight * 10;

        delete link.src;
        delete link.dst;
      });

      Graph.cooldownTicks(300)
        .cooldownTime(20000)
        .nodeAutoColorBy("label")
        //.nodeDesc("desc")
        //.nodeLabel("name")
        .nodeVal("pagerank")
        .linkWidth("weight")
        .linkAutoColorBy("smilar")
        .linkLabel("value")
        .forceEngine("ngraph")
        .graphData(data);
    });
  };
  loadTop500.description = "Top Songs Chart";

  return [loadTop500];
}

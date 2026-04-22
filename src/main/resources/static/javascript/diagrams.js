     function createLine(start, end, options = {}) {
         if (typeof LeaderLine === 'undefined') return null;
         const line = new LeaderLine(start, end, options);
         activeLines.push(line.toString());
         return line;
     }
     // Función auxiliar para crear los DIVs rápidamente
     function createNode(id, text, x, y) {
             let el = document.getElementById(id);
         if (!el) {
             el = document.createElement('div');
             el.id = id;
             el.className = 'nodo'; // Usa tus clases CSS
             el.innerText = text;
             el.style.position = 'absolute';
             el.style.padding = '15px';
             el.style.border = '1px solid black';
             el.style.backgroundColor = 'white';
             el.style.display= 'flex';          // Para centrar el texto
             el.style.alignItems = 'center';      // Centrado Vertical
             el.style.justifyContent = 'center'; // Centrado Horizontal
             el.style.textAlign = 'center';      // Asegura el centro si hay varias líneas
             document.getElementById('canvas').appendChild(el);
         }
         el.style.left = x + 'px';
         el.style.top = y + 'px';
         el.onclick = () => handleNodeClick(el);
         return el;
     }
     
     // --- SISTEMA DE PERSISTENCIA ---
     
     function saveDiagram(activeLines, activeLines2) {
     // 1. Guardar Nodos
     const nodesData = Array.from(document.querySelectorAll('.node, .nodo')).map(el => ({
       id: el.id,
     //              text: el.innerText,
       html: el.innerHTML,
       left: el.style.left,
       top: el.style.top,
       width: el.style.width,
       height: el.style.height,
       bg: el.style.backgroundColor,
       color: el.style.color,
     }));
     
     // 2. Guardar Conexiones (Corregido)
     const connectionsData = connections.map(conn => ({
       startId: conn.start.id,
       endId: conn.end.id,
       anchors: conn.anchors,
       labels: conn.customLabels || null,
       // Usamos valores fijos o recuperados si están disponibles
       options: {
           color: conn.line.color || '#333',
                 startPlug: conn.line.startPlug,
                 size: conn.line.size || 3,
                 dash: conn.line.dash,
                 path: conn.line.path,
                 startLabel: conn.line.startLabel,
                 endLabel: conn.line.endLabel,
                 middleLabel: conn.line.middleLabel,
                 gravity: conn.line.gravity,
                 startSocket: conn.line.startSocket,
                 endSocket: conn.line.endSocket,
                 position: conn.line.position()
       }
     }));
     
     localStorage.setItem('diagram_data', JSON.stringify({
             nodes: nodesData,
             conns: connectionsData,
             count: nodeCount,
             lines: activeLines,
             lines2: activeLines2
     }));
     alert("Guardado correctamente");
     }
     
     function loadDiagram() {
     const data = JSON.parse(localStorage.getItem('diagram_data'));
     if (!data) return;
     
        // Limpiar canvas y líneas
         clearAll(false);
     
         // Restaurar Nodos
         data.nodes.forEach(n => {
             const div = createNode(n.id, n.text, parseInt(n.left), parseInt(n.top));
             div.innerHTML = n.html;
     //                div.innerText = n.text; // Asegura que el texto se muestre correctamente
             div.style.width = n.width;
             div.style.height = n.height;
             div.style.backgroundColor = n.bg;
             div.style.color = n.color;
             div.style.fontFamily = 'Arial, sans-serif';
                
                 // Mantener estilos de centrado que tenías
                 div.style.display = 'flex';
                 div.style.alignItems = 'center';
                 div.style.justifyContent = 'center';
                 div.style.textAlign = 'center';
     
                 makeDraggable(div);
                 div.onclick = () => handleNodeClick(div);
                 document.getElementById('canvas').appendChild(div);
                 
                 // Actualizar el contador global para que no haya IDs duplicados al añadir nuevos
                 const currentIdNum = parseInt(n.id.replace('node-', ''));
                 if (!isNaN(currentIdNum) && currentIdNum >= nodeCount) nodeCount = currentIdNum;
         });
     
     
         nodeCount = data.count || 0;
     
     setTimeout(() => {
       data.conns.forEach(c => {
           const sNode = document.getElementById(c.startId);
           const eNode = document.getElementById(c.endId);
     
           if (sNode && eNode) {
               // Si el JSON dice que hay anchors, los aplicamos
               const startParam = c.anchors && c.anchors.start ? LeaderLine.pointAnchor(sNode, c.anchors.start) : sNode;
               const endParam = c.anchors && c.anchors.end ? LeaderLine.pointAnchor(eNode, c.anchors.end) : eNode;
               // Buscamos si guardamos metadatos de etiquetas en el objeto 'c'
               let lineOpts = {};
               for (let key in c.options) {
                   lineOpts[key] = c.options[key];
               }
               if (c.labels) {
                   if (c.labels.start) {
                       lineOpts.startLabel = LeaderLine.captionLabel(
                           c.labels.start.text, 
                           c.labels.start.options
                       );
                   }
                   if (c.labels.middle) {
                       lineOpts.middleLabel = LeaderLine.captionLabel(
                           c.labels.middle.text, 
                           c.labels.middle.options
                       );
                   }
                         if (c.labels.end) {
                             lineOpts.endLabel = LeaderLine.captionLabel(
                                 c.labels.end.text, 
                                 c.labels.end.options
                             );
                         }
               }
               const newLine = new LeaderLine(startParam, endParam, lineOpts);
               
               // Volvemos a registrar en el array activo
               connections.push({
                   line: newLine,
                   start: sNode,
                   end: eNode,
                   anchors: c.anchors ? c.anchors : null,
                   customLabels: c.labels ? c.labels : null
               });
           }
       });
          const allSvgs = document.querySelectorAll('svg.leader-line');
          allSvgs.forEach(svg => {
              svg.style.zIndex = "9999"; // Aseguras que estén donde tú quieres
          });
     }, 200);
     }
     
     function loadDiagram2() {
     const data = JSON.parse(localStorage.getItem('diagram_storage'));
     if (!data) return;
     
     // Limpiar canvas y líneas
     clearAll(false);
     
     // Restaurar Nodos
     data.nodes.forEach(n => {
       const div = createNode(n.id, n.text, parseInt(n.left), parseInt(n.top));
       div.innerHTML = n.html;
       div.style.width = n.width;
       div.style.height = n.height;
       div.style.backgroundColor = n.bg;
       div.style.color = n.color;
     });
     
     nodeCount = data.count || 0;
     
     // Restaurar Conexiones con delay
     setTimeout(() => {
       data.conns.forEach(c => {
           const s = document.getElementById(c.startId);
           const e = document.getElementById(c.endId);
           if (s && e) {
               const line = new LeaderLine(s, e, c.options);
               connections.push({ line, start: s, end: e });
           }
       });
     }, 200);
     }
     
     function clearAll(deleteStorage = true) {
         // Eliminar líneas visualmente
         connections.forEach(conn => {
     // Verificamos que la conexión y la línea existan antes de llamar a remove()
     if (conn && conn.line) {
        try {
            conn.line.remove();
        } catch (e) {
            console.warn("La línea ya no existía o no se pudo borrar:", e);
        }
     }
     });
         
         // Limpiar arrays
         connections = [];
         activeLines = [];
         activeLines2 = [];
         
         // Eliminar elementos del DOM
         document.querySelectorAll('.node, .nodo').forEach(el => el.remove());
         
         if (deleteStorage) {
             localStorage.removeItem('myFlowchartData');
             nodeCount = 0;
         }
     }
     
     function registerLine(lineObj, startNode, endNode, startPoint = null, endPoint = null) {
         connections.push({
             line: lineObj,
             start: startNode,
             end: endNode,
             // Guardamos las coordenadas como texto para el JSON
             anchors: {
                 start: startPoint, // ej: {x: '50%', y: '100%'}
                 end: endPoint      // ej: {x: '25%', y: '100%'}
             }
         });
     }
     
     function init() {
         initDR();
         initSOH();
         const allSvgs = document.querySelectorAll('svg.leader-line');
         setTimeout(() => {
             allSvgs.forEach(svg => {
                 svg.style.zIndex = "9999"; // Aseguras que estén donde tú quieres
             });
         },200);
     }
     
     function changeFontSize(delta) {
         let canvas = document.getElementById("canvas");
         let todos = [canvas, ...canvas.querySelectorAll("#canvas *:not(h3")];

         todos.forEach(el => {
             // Solo aplicamos a elementos que tengan texto (opcional)
             let currentSize = parseFloat(window.getComputedStyle(el).fontSize);
             el.style.fontSize = (currentSize + delta) + "px";
         });
         let texts = document.getElementsByTagName("text");
         for (letTestElement of texts) {
            let currentSize = parseFloat(window.getComputedStyle(letTestElement).fontSize);
            letTestElement.style.fontSize = (currentSize + delta) + "px";
            }
         refreshDiagram();
     }

     function resetFontSize() { 
        let canvas = document.getElementById("canvas");
        let todos = [canvas, ...canvas.querySelectorAll("#canvas *:not(h3)")];

        todos.forEach(el => {
            el.style.fontSize = 16 + "px";
        });
        let texts = document.getElementsByTagName("text");
        for (letTestElement of texts) {
           letTestElement.style.fontSize = 0.7+"em";
           }
         //document.documentElement.style.fontSize = '13px'; 
         refreshDiagram();
         localStorage.setItem('mq_fontSize', 16); 
     }
     
     function refreshDiagram() {
         connections.forEach(line => {
             line.line.position();
         });
     }

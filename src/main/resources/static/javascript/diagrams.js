let nodeCount = 0;
let connections = [];
let lines = [];
let connectionMode = false;
let selectedNodes = [];

let activeLines = [];
let activeLines2 = [];

function addNode() {
    nodeCount++;
    const canvas = document.getElementById('canvas');
    const div = document.createElement('div');

    div.id = 'node-' + nodeCount;
    div.className = 'nodo';
    div.innerText = 'Nodo ' + nodeCount;
    div.style.left = '50px';
    div.style.top = '50px';

    // Hacerlo arrastrable de forma simple
    makeDraggable(div);

    // Evento para selección de conexión
    div.onclick = () => handleNodeClick(div);

    canvas.appendChild(div);
}

function makeDraggable(el) {
    let pos1 = 0, pos2 = 0, pos3 = 0, pos4 = 0;

    el.onmouseenter = () => {
        el.style.cursor = "grab";
        el.style.zIndex = "5000";
        // Cambiamos zoom por transform
        el.style.transform = "scale(1)"; 
        el.style.transition = "transform 0.2s ease"; // Opcional: para que sea suave
        // Actualiza las líneas
        //connections.forEach(conn => conn.line.position());
    };

    el.onmouseleave = () => {
        el.style.cursor = "default";
        el.style.transform = "scale(1)"; 
        el.style.zIndex = "5000";
        connections.forEach(conn => conn.line.position());
    };
    el.onmousedown = (e) => {
        e.preventDefault();
        pos3 = e.clientX;
        pos4 = e.clientY;
        document.onmouseup = () => {
            document.onmouseup = null;
            document.onmousemove = null;
            el.style.cursor = "grab";
            //el.style.transform = "scale(1)"; 
            connections.forEach(conn => conn.line.position());
        };
        document.onmousemove = (e) => {
            pos1 = pos3 - e.clientX;
            pos2 = pos4 - e.clientY;
            pos3 = e.clientX;
            pos4 = e.clientY;
            el.style.cursor = "grabbing";
            el.style.top = (el.offsetTop - pos2) + "px";
            el.style.left = (el.offsetLeft - pos1) + "px";

            // Actualizar flechas al mover
            connections.forEach(conn => conn.line.position());
        };
    };
}

function startConnection() {
    connectionMode = true;
    selectedNodes = [];
    //alert("Selecciona el primer nodo y luego el segundo.");
}

function handleNodeClick(el) {
    if (!connectionMode) return;

    selectedNodes.push(el);
    el.style.border = "2px solid yellow";

    if (selectedNodes.length === 2) {
        const line = new LeaderLine(
            selectedNodes[0],
            selectedNodes[1],
            { color: '#333', size: 3, path: 'straight' }
        );
        connections.push({ line: line, start: selectedNodes[0], end: selectedNodes[1] });

        // Limpiar selección
        selectedNodes.forEach(n => n.style.border = "none");
        selectedNodes = [];
        connectionMode = false;
    }
}

function pushCconnectionsToActiveLines(canvas, line, start, end, customLabels = null, anchores = null) {
    connections.push({ canvasId: canvas, line: line, start: start, end: end, customLabels: customLabels, anchors: anchores });
    activeLines.push(line.toString());
}

function doLine(canvas, start, end, customLabels = null, color = '#333', size = 3, path = 'grid', dash = null, startLabel = null, middleLabel = null, endLabel = null, anchors = null,plugs = {startPlug: 'disc', endPlug: 'arrow1'}) {
    myLabelData = {
        start: customLabels?.start ? customLabels.start : startLabel ? { text: startLabel, options: {} } : { text: '', options: {} },
        middle: customLabels?.middle ? customLabels.middle : middleLabel ? { text: middleLabel, options: {} } : { text: '', options: {} },
        end: customLabels?.end ? customLabels.end : endLabel ? { text: endLabel, options: {} } : { text: '', options: {} }
    };
    let lineStart = start;
    let lineEnd = end;
    let anchorStart = anchors?.start ? anchors.start : null;
    let anchorEnd = anchors?.end ? anchors.end : null;

    if (anchorStart) {
        lineStart = LeaderLine.pointAnchor(start, anchorStart);
    }
    if (anchorEnd) {
        lineEnd = LeaderLine.pointAnchor(end, anchorEnd);
    }
    const lineX = new LeaderLine(lineStart, lineEnd, {
        startPlug: plugs.startPlug || 'disc',
        endPlug: plugs.endPlug || 'arrow3',
        color: color,
        size: size,
        dash: dash,
        path: path, // Curva suave
        startLabel: LeaderLine.captionLabel(myLabelData.start.text, myLabelData.start.options),
        middleLabel: LeaderLine.captionLabel(myLabelData.middle.text, myLabelData.middle.options),
        endLabel: LeaderLine.captionLabel(myLabelData.end.text, myLabelData.end.options),

    });
    lineX.onclick = () => handleNodeClick(el);
    pushCconnectionsToActiveLines(canvas, lineX, start, end, myLabelData, anchors);
    return lineX;
}

function createLine(start, end, options = {}) {
    if (typeof LeaderLine === 'undefined') return null;
    const line = new LeaderLine(start, end, options);
    activeLines.push(line.toString());
    return line;
}
// Función auxiliar para crear los DIVs rápidamente
function createNode(canvas, id, text, x, y) {
    let el = document.getElementById(id);
    let myCanvas = document.getElementById(canvas);
    if (!el) {
        el = document.createElement('div');
        el.id = id;
        el.className = 'nodo'; // Usa tus clases CSS
        el.innerText = text;
        el.style.position = 'absolute';
        el.style.padding = '15px';
        el.style.border = '1px solid black';
        el.style.backgroundColor = 'white';
        el.style.display = 'flex';          // Para centrar el texto
        el.style.alignItems = 'center';      // Centrado Vertical
        el.style.justifyContent = 'center'; // Centrado Horizontal
        el.style.textAlign = 'center';      // Asegura el centro si hay varias líneas
        el.title = id ; // Tooltip con el mismo texto
    }
    myCanvas.appendChild(el);
    el.style.left = x + 'px';
    el.style.top = y + 'px';
    el.onclick = () => handleNodeClick(el);
    return el;
}

// --- SISTEMA DE PERSISTENCIA ---

function saveDiagram(activeLines, activeLines2) {
    // 1. Guardar Nodos
    const nodesData = Array.from(document.querySelectorAll('.node, .nodo')).map(el => ({
        canvas: el.parentElement.id, // Guardamos el canvas al que pertenece
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
        canvas: conn.canvas ? conn.canvas : null,
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
            endSocket: conn.line.endSocket
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
    let currentIdNum = 0;
    data.nodes.forEach(n => {
        const div = createNode(n.canvas, n.text, parseInt(n.left), parseInt(n.top));
        //div.style.position = 'relative'; // Asegura que el nodo se posicione correctamente
        div.innerHTML = n.html;
        //                div.innerText = n.text; // Asegura que el texto se muestre correctamente
        div.style.width = n.width;
        div.style.height = n.height;
        div.style.backgroundColor = n.bg;
        div.style.color = n.color;
        div.style.fontFamily = 'Arial, sans-serif';
        div.id = n.id; // Asegura que el ID se restaure correctamente
        // Mantener estilos de centrado que tenías
        div.style.display = 'flex';
        div.style.alignItems = 'center';
        div.style.justifyContent = 'center';
        div.style.textAlign = 'center';
        div.style.top = n.top;
        div.style.left = n.left;
        makeDraggable(div);
        div.onclick = () => handleNodeClick(div);
        document.getElementById(n.canvas).appendChild(div);

        // Actualizar el contador global para que no haya IDs duplicados al añadir nuevos
        currentIdNum++;
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
            svg.style.zIndex = "5000"; // Aseguras que estén donde tú quieres
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
    initT3270();
    const allSvgs = document.querySelectorAll('svg.leader-line');
    setTimeout(() => {
        allSvgs.forEach(svg => {
            svg.style.zIndex = "5000"; // Aseguras que estén donde tú quieres
        });
    }, 200);
}

function changeFontSize(delta) {
    let canvas = document.getElementById("canvas");
    let canvas2 = document.getElementById("canvas2");

    // Seleccionamos los hijos de ambos, excluyendo los h3
    let hijos1 = canvas.querySelectorAll("#canvas *:not(h3)");
    let hijos2 = canvas2.querySelectorAll("#canvas2 *:not(h3)");

    // Unimos todo en un solo array: [Padre1, Padre2, ...Hijos1, ...Hijos2]
    let todos = [canvas, canvas2, ...hijos1, ...hijos2];

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
        letTestElement.style.fontSize = 0.7 + "em";
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

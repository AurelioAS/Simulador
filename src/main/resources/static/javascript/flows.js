const kafkaIcon = `
<svg width="24" height="24" viewBox="0 0 24 24" fill="currentColor" xmlns="http://www.w3.org/2000/svg" style="vertical-align: middle; margin-right: 8px;">
    <path d="M20.25 10.125H3.75C3.336 10.125 3 10.461 3 10.875V13.125C3 13.539 3.336 13.875 3.75 13.875H20.25C20.664 13.875 21 13.539 21 13.125V10.875C21 10.461 20.664 10.125 20.25 10.125Z"/>
    <path d="M20.25 4.5H3.75C3.336 4.5 3 4.836 3 5.25V7.5C3 7.914 3.336 8.25 3.75 8.25H20.25C20.664 8.25 21 7.914 21 7.5V5.25C21 4.836 20.664 4.5 20.25 4.5Z"/>
    <path d="M20.25 15.75H3.75C3.336 15.75 3 16.086 3 16.5V18.75C3 19.164 3.336 19.5 3.75 19.5H20.25C20.664 19.5 21 19.164 21 18.75V16.5C21 16.086 20.664 15.75 20.25 15.75Z"/>
</svg>`;

function initDR() {
    // 1. Limpiar líneas previas si las hubiera
    if (typeof clearLines === 'function') clearLines();
    const offset = 0;
    const canvas = document.getElementById('canvas');

    // 2. Definir los nodos (puedes crearlos dinámicamente o usar los que ya tienes)
    const nodeIzq = createNode(canvas.id, 'node-izq', 'Externo', 900, 120 + offset);
    makeDraggable(nodeIzq);
    nodeIzq.style.width = '100px';
    nodeIzq.style.height = '180px';
    nodeIzq.style.backgroundColor = '#2ecc71';
    nodeIzq.innerHTML = '<i class="fas fa-star"></i> Sistema<br>Externo';

    const nodeMQ = createNode(canvas.id, 'node-mq', 'IBM MQ', 700, 182 + offset);
    nodeMQ.style.backgroundColor = '#0062ff';
    nodeMQ.style.color = 'white';
    nodeMQ.innerHTML = '<i class="fas fa-server"></i>&nbsp;IBM MQ';
    makeDraggable(nodeMQ);

    const nodeIzq2 = createNode(canvas.id, 'node-izq2', 'MQLauncher', 500, 120 + offset);
    nodeIzq2.classList.add('bordes-infinitos');
    nodeIzq2.innerHTML = 'MQLauncher<br>(nodeIzq2)';
    nodeIzq2.style.backgroundColor = '#3498db';
    //makeDraggable(nodeIzq2);

    const nodeDer1 = createNode(canvas.id, 'node-der-1', 'IBM MQ', 280, 140 + offset);
    nodeDer1.style.backgroundColor = '#0062ff';
    nodeDer1.style.color = 'white';
    nodeDer1.innerHTML = 'IBM MQ<br>(nodeDer1)';
    makeDraggable(nodeDer1);
    const nodeDer2 = createNode(canvas.id, 'node-der-2', 'IBM MQ', 280, 240 + offset);
    nodeDer2.style.backgroundColor = '#0062ff';
    nodeDer2.style.color = 'white';
    nodeDer2.innerHTML = 'IBM MQ<br>(nodeDer2)';
    makeDraggable(nodeDer2);

    const nodeDer3 = createNode(canvas.id, 'node-der-3', 'Master/Mainframe', 50, 140 + offset);
    nodeDer3.style.backgroundColor = '#e67e22';
    nodeDer3.innerHTML = 'Master<br>Mainframe';
    makeDraggable(nodeDer3);

    const nodeDer4 = createNode(canvas.id, 'node-der-4', 'Secundario/Gravity', 50, 240 + offset);
    nodeDer4.style.backgroundColor = '#e67e22';
    nodeDer4.innerHTML = 'Secundario<br>Gravity';
    makeDraggable(nodeDer4);

    const nodeHz = createNode(canvas.id, 'node-Hz', 'Hazelcast', 700, 270 + offset);
    nodeHz.style.backgroundColor = '#e67e22';
    nodeHz.style.color = 'white';
    makeDraggable(nodeHz);

    const nodeKafka = createNode(canvas.id, 'node-Kf', 'Kafka', 700, 350 + offset);
    nodeKafka.style.backgroundColor = '#3498db';
    nodeKafka.style.color = 'white';
    nodeKafka.innerHTML = `${kafkaIcon} <strong>Apache Kafka</strong>`;
    makeDraggable(nodeKafka);


    let myLabelData = {};
    let plugs = {};
    // 3. Unir con LeaderLine
    if (typeof LeaderLine !== 'undefined') {
        let pStart = {};
        let pEnd = {};

        myLabelData = {
            end: { text: 'QUEUE0001', options: { color: 'blue', offset: [-1, 0] } }
        };
        const line0 = doLine(canvas.id, nodeIzq, nodeMQ, myLabelData, '#3498db', 3, 'grid', null, null, null, myLabelData.end);

        myLabelData = {
            start: { text: 'GETI_QUEUE', options: { color: 'blue', offset: [-90, 0] } },
        };
        const lineMQ = doLine(canvas.id, nodeMQ, nodeIzq2, myLabelData, '#3498db', 3, 'grid', null, myLabelData.start, null, null);

        const line1 = doLine(canvas.id, nodeIzq2, nodeDer1, {}, '#3498db', 3, 'grid', null, null, null, 'To Mainframe');

        pStart = { x: '0%', y: '72%' };
        pEnd = { x: '100%', y: '50%' };
        const line2 = doLine(canvas.id, nodeIzq2, nodeDer2, {}, '#3498db', 3, 'grid', { len: 5, gap: 10, animation: true }, null, null, 'To Gravity', anchors = { start: pStart, end: pEnd });


        myLabelData = {
            middle: { text: 'From Mainframe', options: { color: 'red', fontSize: 10 } },
            end: { text: 'QUEUE', options: { color: 'red', offset: [-1, -10] } }
        };

        const line4 = doLine(canvas.id, nodeDer1, nodeIzq2, myLabelData, '#e74c3c', 3, 'grid', { len: 5, gap: 10, animation: true }, null, myLabelData.middle, myLabelData.end);
        line4.setOptions({ startSocket: 'top', endSocket: 'top' });

        myLabelData = {
            start: { text: 'QUEUE02', options: { color: 'red', offset: [-1, 0] } },
            middle: { text: 'From Gravity', options: { color: 'red', fontSize: 10 } },
            end: { text: 'QUEUE01', options: { color: 'red', offset: [-30, 0] } }
        };
        const line5 = doLine(canvas.id, nodeDer2, nodeIzq2, myLabelData, '#e74c3c', 3, 'grid', { len: 10, gap: 10, animation: true }, myLabelData.start, myLabelData.middle, myLabelData.end);
        line5.setOptions({ startSocket: 'bottom', endSocket: 'bottom' });
        
        myLabelData = {};
        const line6 = doLine(canvas.id, nodeDer1, nodeDer3, {}, '#3498db', 3, 'straight', null, null, null, null);
        line6.setOptions({ startSocket: 'left', endSocket: 'rigth' });

        const line7 = doLine(canvas.id, nodeDer2, nodeDer4, {}, '#3498db', 3, 'grid', { len: 5, gap: 10, animation: true }, null, null, null);
        line7.setOptions({ startSocket: 'left', endSocket: 'rigth' });
        //Vuelta
         pStart = { x: '50%', y: '0%' };
         pEnd = { x: '25%', y: '0%' };
        const line8 = doLine(canvas.id, nodeDer3, nodeDer1, {}, '#e74c3c', 3, 'grid', { len: 5, gap: 10, animation: true }, null, null, null, anchors = { start: pStart, end: pEnd });
        line8.setOptions({ startSocket: 'top', endSocket: 'top' });

        pStart = { x: '50%', y: '100%' };
        pEnd = { x: '25%', y: '100%' };
        const line9 = doLine(canvas.id, nodeDer4, nodeDer2, {}, '#e74c3c', 3, 'grid', { len: 10, gap: 10, animation: true }, null, null, null, anchors = { start: pStart, end: pEnd });
        line9.setOptions({ startSocket: 'bottom', endSocket: 'bottom' });

        pStart = { x: '100%', y: '40%' };
        pEnd = { x: '0%', y: '25%' };
        myLabelData = {
            end: { text: 'PUT_QUEUE', options: { color: 'red', offset: [-20, -42] } },
        };
        const line10 = doLine(canvas.id, nodeIzq2, nodeMQ, myLabelData, '#e74c3c', 3, 'grid', { len: 5, gap: 10, animation: true }, null, null, myLabelData.end, anchors = { start: pStart, end: pEnd });
        //line10.setOptions({ startSocket: 'top', endSocket: 'top' });

        pStart = { x: '100%', y: '21%' };
        pEnd = { x: '0%', y: '41%' };
        myLabelData = {
            start: { text: 'GET_QUEUE', options: { color: 'red', offset: [-1, -20] } },
        };
        const line11 = doLine(canvas.id, nodeMQ, nodeIzq, myLabelData, '#e74c3c', 3, 'grid', { len: 5, gap: 10, animation: true }, myLabelData.start, null, null, anchors = { start: pStart, end: pEnd });

        pStart = { x: '100%', y: '81%' };
        pEnd = { x: '0%', y: '41%' };
        plugs ={
            startPlug: 'arrow1',
            endPlug: 'arrow1',
        }
        myLabelData = {};
        const line12 = doLine(canvas.id, nodeIzq2, nodeHz, myLabelData, '#43AB7A', 3, 'grid', null, myLabelData.start, null, null, anchors = { start: pStart, end: pEnd }, plugs);

        pStart = { x: '80%', y: '100%' };
        pEnd = { x: '0%', y: '41%' };
        plugs ={};
        myLabelData = {};
        const line13 = doLine(canvas.id, nodeIzq2, nodeKafka, myLabelData, '#3498db', 3, 'grid', null, myLabelData.start, null, null, anchors = { start: pStart, end: pEnd });

        activeLines.push(line0.toString(), lineMQ.toString(), line1.toString(), line2.toString(), line4.toString(), line5.toString(), line6.toString(), line7.toString(), line8.toString(), line9.toString(), line10.toString(), line11.toString(), line12.toString(), line13.toString());
    }
}

function initSOH() {
    // 1. Limpiar líneas previas si las hubiera
    if (typeof clearLines === 'function') clearLines();

    const canvas = document.getElementById('canvas2');
    const offset = 0; // Ajusta según tu diseño
    // 2. Definir los nodos (puedes crearlos dinámicamente o usar los que ya tienes)
    const nodeIzq = createNode(canvas.id, 'sohNode-izq', 'Externo', 900, 120 + offset);
    makeDraggable(nodeIzq);
    nodeIzq.style.width = '100px';
    nodeIzq.style.height = '180px';
    nodeIzq.style.backgroundColor = '#2ecc71';
    nodeIzq.innerHTML = 'Sistema<br>Externo';

    const nodeMQ = createNode(canvas.id, 'sohNode-mq', 'IBM MQ', 700, 182 + offset);
    nodeMQ.style.backgroundColor = '#0062ff';
    nodeMQ.style.color = 'white';
    makeDraggable(nodeMQ);

    const nodeIzq2 = createNode(canvas.id, 'sohNode-izq2', 'MQLauncher', 500, 120 + offset);
    nodeIzq2.classList.add('bordes-infinitos');
    nodeIzq2.style.backgroundColor = '#3498db';
    //makeDraggable(nodeIzq2);

    const nodeDer1 = createNode(canvas.id, 'sohNode-der-1', 'IBM MQ', 300, 150 + offset);
    nodeDer1.style.backgroundColor = '#0062ff';
    nodeDer1.style.color = 'white';
    makeDraggable(nodeDer1);
    const nodeDer2 = createNode(canvas.id, 'sohNode-der-2', 'IBM MQ', 300, 250 + offset);
    nodeDer2.style.backgroundColor = '#0062ff';
    nodeDer2.style.color = 'white';
    makeDraggable(nodeDer2);

    const nodeDer3 = createNode(canvas.id, 'sohNode-der-3', 'Master/Mainframe', 50, 130 + offset);
    nodeDer3.style.backgroundColor = '#e67e22';
    nodeDer3.innerHTML = '<i class="fas fa-star"></i> Master<br>Mainframe';
    makeDraggable(nodeDer3);
    const nodeDer4 = createNode(canvas.id, 'sohNode-der-4', 'Secundario/Gravity', 50, 240 + offset);
    nodeDer4.style.backgroundColor = '#e67e22';
    nodeDer4.innerHTML = '<i class="fas fa-star"></i> Secundario<br>Mainframe';
    makeDraggable(nodeDer4);

    const nodeHz = createNode(canvas.id, 'node-Hz2', 'Hazelcast', 700, 270 + offset);
    nodeHz.style.backgroundColor = '#e67e22';
    nodeHz.style.color = 'white';
    makeDraggable(nodeHz);

    const nodeKafka = createNode(canvas.id, 'node-Kf2', 'Kafka', 700, 350 + offset);
    nodeKafka.style.backgroundColor = '#3498db';
    nodeKafka.style.color = 'white';
    nodeKafka.innerHTML = `${kafkaIcon} <strong>Apache Kafka</strong>`;
    makeDraggable(nodeKafka);
    
    // 3. Unir con LeaderLine
    if (typeof LeaderLine !== 'undefined') {
        let pStart = {};
        let pEnd = {};
        let myLabelData = {
            start: { text: 'PUT_QUEUE01', options: { color: 'blue', offset: [-30, 0] } }
        };
        const line0 = doLine(canvas.id, nodeMQ, nodeIzq, myLabelData, '#3498db', 3, 'grid', null, myLabelData.start, null, null);

        myLabelData = {
            end: { text: 'GETsQUEUE', options: { color: 'blue', offset: [-50, 0] } },
        };
        const lineMQ = doLine(canvas.id, nodeIzq2, nodeMQ, myLabelData, '#3498db', 3, 'grid', null, null, null, myLabelData.end);

        const line1 = doLine(canvas.id, nodeDer1, nodeIzq2, {}, '#3498db', 3, 'grid', null, 'From Mainframe', null, null);

        pStart = { x: '100%', y: '42%' };
        pEnd = { x: '0%', y: '75%' };
        const line2 = doLine(canvas.id, nodeDer2, nodeIzq2, {}, '#3498db', 3, 'grid', { len: 5, gap: 10, animation: true }, 'From Gravity', null, null, anchors = { start: pStart, end: pEnd });

        myLabelData = {
            middle: { text: 'To MQ', options: { color: 'red', offset: [-1, -10] } },
        };
        const line4 = doLine(canvas.id, nodeIzq2, nodeDer1, myLabelData, '#e74c3c', 3, 'grid', { len: 5, gap: 10, animation: true }, null, myLabelData.middle, null);
        line4.setOptions({ startSocket: 'top', endSocket: 'top' });

        const line6 = doLine(canvas.id, nodeDer3, nodeDer1, {}, '#3498db', 3, 'grid', null, null, null, null);
        line6.setOptions({ startSocket: 'right', endSocket: 'rigth' });

        const line7 = doLine(canvas.id, nodeDer4, nodeDer2, {}, '#3498db', 3, 'grid', { len: 5, gap: 10, animation: true }, null, null, null);
        line7.setOptions({ startSocket: 'right', endSocket: 'rigth' });
        //Vuelta
        pStart = { x: '30%', y: '0%' };
        pEnd = { x: '50%', y: '0%' };
        const line8 = doLine(canvas.id, nodeDer1, nodeDer3, {}, '#e74c3c', 3, 'grid', { len: 5, gap: 10, animation: true }, null, null, null, anchors = { start: pStart, end: pEnd });
        line8.setOptions({ startSocket: 'top', endSocket: 'top' });

        pStart = { x: '0%', y: '20%' };
        pEnd = { x: '100%', y: '40%' };
        const line10 = doLine(canvas.id, nodeMQ, nodeIzq2, {}, '#e74c3c', 3, 'grid', { len: 5, gap: 10, animation: true }, null, null, null, anchors = { start: pStart, end: pEnd });
        //line10.setOptions({ startSocket: 'top', endSocket: 'top' });

        pStart = { x: '0%', y: '42%' };
        pEnd = { x: '100%', y: '25%' };
        myLabelData = {
            start: { text: 'QUEUE_NAME', options: { color: 'red', offset: [-1, -10] } },
        };
        const line11 = doLine(canvas.id, nodeIzq, nodeMQ, myLabelData, '#e74c3c', 3, 'grid', { len: 5, gap: 10, animation: true }, myLabelData.start, null, null, anchors = { start: pStart, end: pEnd });

        pStart = { x: '50%', y: '100%' };
        pEnd = { x: '60%', y: '100%' };
        myLabelData = {
            middle: { text: 'To MQ2', options: { color: 'red', offset: [-1, -10] } },
        };
        
        const line12 = doLine(canvas.id, nodeIzq2, nodeDer2, myLabelData, '#e74c3c', 3, 'grid', { len: 5, gap: 10, animation: true }, null, myLabelData.middle, null, anchors = { start: pStart, end: pEnd });
        line12.setOptions({ startSocket: 'bottom', endSocket: 'bottom' });

        pStart ={ x: '40%', y: '100%' };
        pEnd = { x: '50%', y: '100%' };
        myLabelData = {
            middle: { text: 'To MQ3', options: { color: 'red', offset: [-1, -10] } },
        };
        const line13 = doLine(canvas.id, nodeDer2, nodeDer4, myLabelData, '#e74c3c', 3, 'grid', { len: 5, gap: 10, animation: true }, null, myLabelData.middle, null, anchors = { start: pStart, end: pEnd });
        line13.setOptions({ startSocket: 'bottom', endSocket: 'bottom' });
        // Guardar para poder borrarlas o moverlas luego

        pStart = { x: '100%', y: '81%' };
        pEnd = { x: '0%', y: '41%' };
        plugs ={
            startPlug: 'arrow1',
            endPlug: 'arrow1',
        }
        myLabelData = {};
        const line14 = doLine(canvas.id, nodeIzq2, nodeHz, myLabelData, '#43AB7A', 3, 'grid', null, myLabelData.start, null, null, anchors = { start: pStart, end: pEnd }, plugs);

        pStart = { x: '80%', y: '100%' };
        pEnd = { x: '0%', y: '41%' };
        plugs ={};
        myLabelData = {};
        const line15 = doLine(canvas.id, nodeIzq2, nodeKafka, myLabelData, '#3498db', 3, 'grid', null, myLabelData.start, null, null, anchors = { start: pStart, end: pEnd });

        activeLines2.push(line0.toString(), lineMQ.toString(), line1.toString(), line2.toString(), line4.toString(), line6.toString(), line7.toString(), line8.toString(), line10.toString(), line11.toString(), line12.toString(), line13.toString(), line14.toString(), line15.toString());
    }
}

function initT3270() {
    // 1. Limpiar líneas previas si las hubiera
    if (typeof clearLines === 'function') clearLines();

    const canvas = document.getElementById('canvas3');
    const offset = 0; // Ajusta según tu diseño
    // 2. Definir los nodos (puedes crearlos dinámicamente o usar los que ya tienes)

    const nodeDer3 = createNode(canvas.id, 't32Node-der-3', 'Master/Mainframe', 50, 140 + offset);
    nodeDer3.style.backgroundColor = '#e67e22';
    nodeDer3.innerHTML = '<i class="fas fa-star"></i> Master<br>Mainframe';
    makeDraggable(nodeDer3);
    const nodeDer4 = createNode(canvas.id, 't32Node-der-4', 'Secundario/Gravity', 50, 240 + offset);
    nodeDer4.style.backgroundColor = '#e67e22';
    nodeDer4.innerHTML = '<i class="fas fa-star"></i> Secundario<br>Mainframe';
    makeDraggable(nodeDer4);

    const nodeDer2 = createNode(canvas.id, 'T32Node-der-2', 'IBM MQ', 300, 140 + offset);
    nodeDer2.style.backgroundColor = '#0062ff';
    nodeDer2.style.color = 'white';
    nodeDer2.style.width = '100px';
    nodeDer2.style.height = '180px';
    makeDraggable(nodeDer2);

    const nodeIzq2 = createNode(canvas.id, 't32Node-izq2', 'MQLauncher', 550, 140 + offset);
    nodeIzq2.style.backgroundColor = '#3498db';
    nodeIzq2.classList.add('bordes-infinitos');
    nodeIzq2.innerHTML = 'MQLauncher<br>(Comparer)';
    //makeDraggable(nodeIzq2);

    const nodeHz = createNode(canvas.id, 'node-Hz3', 'Hazelcast', 800, 150 + offset);
    nodeHz.style.backgroundColor = '#e67e22';
    nodeHz.style.color = 'white';
    makeDraggable(nodeHz);

    const nodeKafka = createNode(canvas.id, 'node-Kf3', 'Kafka', 800, 230 + offset);
    nodeKafka.style.backgroundColor = '#3498db';
    nodeKafka.style.color = 'white';
    nodeKafka.innerHTML = `${kafkaIcon} <strong>Apache Kafka</strong>`;
    makeDraggable(nodeKafka);

    // 3. Unir con LeaderLine
    if (typeof LeaderLine !== 'undefined') {
        let pStart = {};
        let pEnd = {};
        let myLabelData = {
            end: { text: 'PUT_QUEUE01', options: { color: 'blue', offset: [-100, 0] } }
        };
        pStart = { x: '100%', y: '50%' };
        pEnd = { x: '0%', y: '30%' };
        const line0 = doLine(canvas.id, nodeDer3, nodeDer2, myLabelData, '#3498db', 3, 'grid', null, null, null, myLabelData.end, anchors = { start: pStart, end: pEnd });

        myLabelData = {
            end: { text: 'PUT_QUEUE02', options: { color: 'blue', offset: [-100, 0] } }
        };
        pStart = { x: '100%', y: '50%' };
        pEnd = { x: '0%', y: '70%' };
        const line1 = doLine(canvas.id, nodeDer4, nodeDer2, myLabelData, '#3498db', 3, 'grid', null, null, null, myLabelData.end, anchors = { start: pStart, end: pEnd });

        pStart = { x: '50%', y: '100%' };
        pEnd = { x: '70%', y: '100%' };
        myLabelData = {
            end: { text: 'FWD_QUEUE01', options: { color: 'blue', offset: [10, 0] } }
        };
        const line2 = doLine(canvas.id, nodeIzq2, nodeDer2, myLabelData, '#e74c3c', 3, 'grid', { len: 5, gap: 10, animation: true }, null, null, null, anchors = { start: pStart, end: pEnd });
        line2.setOptions({ startSocket: 'bottom', endSocket: 'bottom' });

        pStart = { x: '30%', y: '100%' };
        pEnd = { x: '70%', y: '100%' };
        myLabelData = {
            start: { text: 'FWD_QUEUE01', options: { color: 'blue', offset: [-100, 0] } }
        };
        const line3 = doLine(canvas.id, nodeDer2, nodeDer4, myLabelData, '#e74c3c', 3, 'grid', { len: 5, gap: 10, animation: true }, null, null, null, anchors = { start: pStart, end: pEnd });
        line3.setOptions({ startSocket: 'bottom', endSocket: 'bottom' });

        myLabelData = {
            start: { text: 'GET_QUEUE01', options: { color: 'blue', offset: [-0, 0] } }
        };
        pStart = { x: '100%', y: '20%' };
        pEnd = { x: '0%', y: '30%' };
        const line4 = doLine(canvas.id, nodeDer2, nodeIzq2, myLabelData, '#3498db', 3, 'grid', null, myLabelData.start, null, null, anchors = { start: pStart, end: pEnd });

        myLabelData = {
            start: { text: 'GET_QUEUE02', options: { color: 'blue', offset: [-0, 0] } }
        };
        pStart = { x: '100%', y: '80%' };
        pEnd = { x: '0%', y: '70%' };
        const line5 = doLine(canvas.id, nodeDer2, nodeIzq2, myLabelData, '#3498db', 3, 'grid', null, myLabelData.start, null, null, anchors = { start: pStart, end: pEnd });
        
        pStart = { x: '100%', y: '30%' };
        pEnd = { x: '0%', y: '50%' };
        plugs ={
            startPlug: 'arrow1',
            endPlug: 'arrow1',
        }
        myLabelData = {};
        const line14 = doLine(canvas.id, nodeIzq2, nodeHz, myLabelData, '#43AB7A', 3, 'grid', null, myLabelData.start, null, null, anchors = { start: pStart, end: pEnd }, plugs);

        pStart = { x: '100%', y: '60%' };
        pEnd = { x: '0%', y: '50%' };
        plugs ={};
        myLabelData = {};
        const line15 = doLine(canvas.id, nodeIzq2, nodeKafka, myLabelData, '#3498db', 3, 'grid', null, myLabelData.start, null, null, anchors = { start: pStart, end: pEnd });

        // Guardar para poder borrarlas o moverlas luego
        activeLines2.push(line0.toString(), line1.toString(), line2.toString(), line3.toString(), line4.toString(), line5.toString(), line14.toString(), line15.toString());
    }
}

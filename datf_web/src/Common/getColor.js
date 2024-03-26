
export default function getColor(i) {
    /* const scale = chroma.scale(['#a9ce7d', '#50C878']);
    return scale(i/5).hex() */
    const colors = ['#574e91','#f0b41bcc',  '#5ED5D6', '#C37DC4', '#B2CF65', '#062277', '#e08563', '#db6456', '#d43d51','#5f4a8bff','#5D9BD8','#AC1B36','#c6dbef','#9ecae1','#6baed6','#4292c6','#BEA528',"#BF8BC6",'#329F2D','#67749C','#0BC5A3','#462107',"#9E1385",'#0F0FF4','#3D8638','#1AA4EE','#B11219','#BD8408','#57506A','#E697E3','#22F151','#CDCC63','#8131F5','#35C840','#CD930D','#89114D','#FCCC05','#94A9BB','#0F5441','#B57BF3','#F908E3','#B8E6F3','#3F9B1D','#F13F2A','#D1968F','#C41703','#D4D4D3','#9B9B6B','#A0EA27','#D7FC9A','#9CF607','#D6F6A2','#4D730F','#77B90C','#607342','#11900D','#ADB4AD','#52AB52','#10E7B0','#C2DED7','#1DB690','#46FBCE','#636D6A','#128965'];
    // const colors = ['#5f4a8bff', '#B2CF65','#e69a8dff', '#5ED5D6', '#C37DC4',  '#e3a379', '#e08563', '#db6456', '#d43d51'];
    // const colors = ['#f7fbff','#deebf7','#c6dbef','#9ecae1','#6baed6','#4292c6'].reverse()
    return colors[i]
}
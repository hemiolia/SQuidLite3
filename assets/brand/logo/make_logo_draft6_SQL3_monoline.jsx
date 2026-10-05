function rgb(h){var c=new RGBColor();c.red=parseInt(h.substr(1,2),16);c.green=parseInt(h.substr(3,2),16);c.blue=parseInt(h.substr(5,2),16);return c;}
var BLUE=rgb("#2A0063"),GOLD=rgb("#BD9A45"),GOLD_D=rgb("#9A7A2E"),MILK=rgb("#FFFFF5"),PAPER=rgb("#FBF5EC");
var SW=34, AW=1500, AH=560, GAP=80;
var doc=app.documents.add(DocumentColorSpace.RGB,AW,AH);
var NAMES=["P1_文字だけ","P2_イカのQ","P3_三本線","P5_丸まる一本足"];
var VAR=["plain","squid","inline","curl"];
doc.artboards[0].artboardRect=[0,AH,AW,0]; doc.artboards[0].name=NAMES[0];
for(var i=1;i<4;i++){var y0=-i*(AH+GAP);var a=doc.artboards.add([0,y0+AH,AW,y0]);a.name=NAMES[i];}

// 骨格（単位: 字高150、基線0、線の中心）
var SK_S=[[76,118.5,76,118.5,70.6,127.5],[50,133,60.5,133,33.4,133],[20,104,20,120,20,88],[50,75,33.4,75,66.6,75],[80,46,80,62,80,30],[50,17,66.6,17,39.5,17],[24,31.5,29.4,22.5,24,31.5]];
var SK_3=[[24,118.5,24,118.5,29.4,127.5],[50,133,39.5,133,66.6,133],[80,104,80,120,80,88],[42,75,60,75,60,75,"c"],[80,46,80,62,80,30],[50,17,66.6,17,39.5,17],[24,31.5,29.4,22.5,24,31.5]];
var SK_L=[[17,133],[17,17],[80,17]];
var SK_L_TENT=[[17,133],[17,-26],[206,-26,190,-26,222,-26],[244,-4,240,-18,248,10],[230,18,242,20,220,16],[221,7,221,12,221,7]];

function setStroke(p,sw,col){p.filled=false;p.stroked=true;p.strokeColor=col;p.strokeWidth=sw;p.strokeCap=StrokeCap.ROUNDENDCAP;p.strokeJoin=StrokeJoin.ROUNDENDJOIN;return p;}
function mkPath(g,pts,sw,col,ox,oy,s){
  var p=g.pathItems.add();
  for(var i=0;i<pts.length;i++){var a=pts[i];var q=p.pathPoints.add();var ax=ox+a[0]*s,ay=oy+a[1]*s;q.anchor=[ax,ay];
    if(a.length>=6){q.leftDirection=[ox+a[2]*s,oy+a[3]*s];q.rightDirection=[ox+a[4]*s,oy+a[5]*s];q.pointType=(a.length>6)?PointType.CORNER:PointType.SMOOTH;}
    else{q.leftDirection=[ax,ay];q.rightDirection=[ax,ay];q.pointType=PointType.CORNER;}}
  p.closed=false;return setStroke(p,sw*s,col);}
// 自動で滑らかにする（足）
function smooth(pts){var r=[];for(var i=0;i<pts.length;i++){var p=pts[i],a=pts[Math.max(0,i-1)],b=pts[Math.min(pts.length-1,i+1)];var tx=(b[0]-a[0])*0.22,ty=(b[1]-a[1])*0.22;
  if(i==0){tx=(b[0]-p[0])*0.35;ty=(b[1]-p[1])*0.35;r.push([p[0],p[1],p[0],p[1],p[0]+tx,p[1]+ty]);}
  else if(i==pts.length-1){tx=(p[0]-a[0])*0.35;ty=(p[1]-a[1])*0.35;r.push([p[0],p[1],p[0]-tx,p[1]-ty,p[0],p[1]]);}
  else r.push([p[0],p[1],p[0]-tx,p[1]-ty,p[0]+tx,p[1]+ty]);}return r;}
function mkRing(g,cx,cy,r,sw,col,ox,oy,s){var e=g.pathItems.ellipse(oy+(cy+r)*s,ox+(cx-r)*s,2*r*s,2*r*s);return setStroke(e,sw*s,col);}
function mkDot(g,cx,cy,r,col,ox,oy,s){var e=g.pathItems.ellipse(oy+(cy+r)*s,ox+(cx-r)*s,2*r*s,2*r*s);e.filled=true;e.fillColor=col;e.stroked=false;return e;}

function crSample(c,n){var out=[];for(var i=0;i<c.length-1;i++){var p0=c[Math.max(0,i-1)],p1=c[i],p2=c[i+1],p3=c[Math.min(c.length-1,i+2)];
  for(var k=0;k<n;k++){var t=k/n,t2=t*t,t3=t2*t;out.push([0.5*((2*p1[0])+(-p0[0]+p2[0])*t+(2*p0[0]-5*p1[0]+4*p2[0]-p3[0])*t2+(-p0[0]+3*p1[0]-3*p2[0]+p3[0])*t3),
    0.5*((2*p1[1])+(-p0[1]+p2[1])*t+(2*p0[1]-5*p1[1]+4*p2[1]-p3[1])*t2+(-p0[1]+3*p1[1]-3*p2[1]+p3[1])*t3)]);}}
  out.push(c[c.length-1]);return out;}
function taper(g,ctrl,w0,w1,col,ox,oy,s){var P=crSample(ctrl,14),N=P.length,L=[],R=[];
  for(var i=0;i<N;i++){var a=P[Math.max(0,i-1)],b=P[Math.min(N-1,i+1)];var tx=b[0]-a[0],ty=b[1]-a[1],l=Math.sqrt(tx*tx+ty*ty)||1;var nx=-ty/l,ny=tx/l;var w=(w0+(w1-w0)*Math.pow(i/(N-1),0.9))/2;
    L.push([P[i][0]+nx*w,P[i][1]+ny*w]);R.push([P[i][0]-nx*w,P[i][1]-ny*w]);}
  var pts=L.concat(R.reverse());var p=g.pathItems.add();var arr=[];for(var j=0;j<pts.length;j++)arr.push([ox+pts[j][0]*s,oy+pts[j][1]*s]);p.setEntirePath(arr);p.closed=true;p.filled=true;p.fillColor=col;p.stroked=false;
  var e=P[N-1];mkDot(g,e[0],e[1],w1/2,col,ox,oy,s);return p;}
function drop(g,cx,cy,r,col,ox,oy,s){var k=0.5523*r;var p=g.pathItems.add();var D=[[cx,cy+2.3*r,cx,cy+2.3*r,cx,cy+2.3*r],[cx+r,cy,cx+r,cy+0.9*r,cx+r,cy-k],[cx,cy-r,cx+k,cy-r,cx-k,cy-r],[cx-r,cy,cx-r,cy-k,cx-r,cy+0.9*r]];
  for(var i=0;i<D.length;i++){var a=D[i];var q=p.pathPoints.add();q.anchor=[ox+a[0]*s,oy+a[1]*s];q.leftDirection=[ox+a[2]*s,oy+a[3]*s];q.rightDirection=[ox+a[4]*s,oy+a[5]*s];}
  p.closed=true;p.filled=true;p.fillColor=col;p.stroked=false;return p;}
// 文字の線（三本線なら3層）
function letter(g,pts,dx,dy,col,sc,ox,oy,s,inl){
  if(inl){mkPath(g,pts,SW*1.18,col,ox+dx*s,oy+dy*s,s);mkPath(g,pts,SW*0.70,sc.bg,ox+dx*s,oy+dy*s,s);mkPath(g,pts,SW*0.20,col,ox+dx*s,oy+dy*s,s);}
  else mkPath(g,pts,SW,col,ox+dx*s,oy+dy*s,s);}
function ring(g,cx,cy,r,col,sc,ox,oy,s,inl){
  if(inl){mkRing(g,cx,cy,r,SW*1.18,col,ox,oy,s);mkRing(g,cx,cy,r,SW*0.70,sc.bg,ox,oy,s);mkRing(g,cx,cy,r,SW*0.20,col,ox,oy,s);}
  else mkRing(g,cx,cy,r,SW,col,ox,oy,s);}

// マーク SQ/L3（左下が原点、単位 s 倍）
function mark(parent,ox,oy,s,sc,v){
  var g=parent.groupItems.add(); var inl=(v=="inline"); var R=108, T=168, qx=R+75, qy=T+75;
  letter(g,SK_S,0,T,sc.fg,sc,ox,oy,s,inl);
  ring(g,qx,qy,58,sc.fg,sc,ox,oy,s,inl);
  if(v=="squid"){
    taper(g,[[214,195],[234,160],[240,108],[233,62],[217,44],[205,52]],28,7,sc.fg,ox,oy,s);
    taper(g,[[230,210],[254,176],[264,126],[262,96],[250,84]],22,6,sc.fg,ox,oy,s);
    if(sc.eyeWhite){mkDot(g,qx-18,qy+8,11,sc.eyeWhite,ox,oy,s);mkDot(g,qx+18,qy+8,11,sc.eyeWhite,ox,oy,s);mkDot(g,qx-16,qy+6,5.5,sc.pupil,ox,oy,s);mkDot(g,qx+20,qy+6,5.5,sc.pupil,ox,oy,s);}
    else {mkDot(g,qx-18,qy+8,8.5,sc.fg,ox,oy,s);mkDot(g,qx+18,qy+8,8.5,sc.fg,ox,oy,s);}
  } else if(v=="inline"){
    mkPath(g,smooth([[205.5,203.5],[224,150],[222,72],[210,54]]),7,sc.fg,ox,oy,s);
    mkPath(g,smooth([[214,212],[240,150],[240,82],[228,66]]),7,sc.fg,ox,oy,s);
    mkPath(g,smooth([[222.5,220.5],[256,160],[258,102],[248,88]]),7,sc.fg,ox,oy,s);
  } else if(v=="curl"){
    taper(g,[[214.6,194.4],[240,176],[258,148],[262,118],[250,98],[232,100],[228,116],[238,124]],30,9,sc.fg,ox,oy,s);
    drop(g,246,52,10,sc.accent,ox,oy,s);
  } else {
    mkPath(g,[[211,215],[245,181]],SW,sc.fg,ox,oy,s);
  }
  letter(g,SK_L,0,0,sc.fg,sc,ox,oy,s,inl);
  letter(g,SK_3,(v=="plain")?131:R,0,sc.accent,sc,ox,oy,s,inl);
  return g;}

function word(parent,str,size,sc){var t=parent.textFrames.add();t.contents=str;var a=t.textRange.characterAttributes;a.size=size;a.textFont=app.textFonts.getByName("FuturaPT-Bold");a.fillColor=sc.fg;t.textRange.characters[str.length-1].characterAttributes.fillColor=sc.accent;return t.createOutline();}
function fitInto(item,left,top,w,h){var vb=item.visibleBounds;var iw=vb[2]-vb[0],ih=vb[1]-vb[3];var f=Math.min(w/iw,h/ih)*100;item.resize(f,f,true,true,true,true,f,Transformation.CENTER);vb=item.visibleBounds;iw=vb[2]-vb[0];ih=vb[1]-vb[3];item.translate(left+(w-iw)/2-vb[0],top-(h-ih)/2-vb[1]);}

var ON_PAPER={fg:BLUE,accent:GOLD_D,bg:PAPER};
var ON_BLUE={fg:MILK,accent:GOLD,bg:BLUE,eyeWhite:MILK,pupil:BLUE};
for(var k=0;k<4;k++){
  var r=doc.artboards[k].artboardRect; var L0=r[0],T0=r[1];
  var bg=doc.pathItems.rectangle(T0,L0,AW,AH);bg.filled=true;bg.fillColor=PAPER;bg.stroked=false;
  var v=VAR[k];
  // 1) 紙の上のマーク
  var m=mark(doc,0,0,1,ON_PAPER,v); fitInto(m,L0+50,T0-50,330,460);
  // 2) 横組み
  var g2=doc.groupItems.add(); var m2=mark(g2,0,0,1,ON_PAPER,v); fitInto(m2,L0+440,T0-170,150,190);
  var w=word(g2,"SQuidLite3",64,ON_PAPER); var wb=w.visibleBounds; w.translate(L0+610-wb[0],T0-232-wb[1]);
  // 3) アイコン 300
  var ix=L0+1030,iy=T0-60,IS=300;
  var gi=doc.groupItems.add(); var sq=gi.pathItems.roundedRectangle(iy,ix,IS,IS,66,66);sq.filled=true;sq.fillColor=BLUE;sq.stroked=false;
  var mi=mark(gi,0,0,1,ON_BLUE,v); fitInto(mi,ix,iy,IS,IS); var f=0.68; mi.resize(f*100,f*100,true,true,true,true,f*100,Transformation.CENTER);
  // 4) 小さいアイコン 64 と 32
  var d1=gi.duplicate(); d1.resize(64/IS*100,64/IS*100,true,true,true,true,64/IS*100,Transformation.CENTER); var b1=d1.visibleBounds; d1.translate(L0+1030-b1[0],T0-400-b1[1]);
  var d2=gi.duplicate(); d2.resize(32/IS*100,32/IS*100,true,true,true,true,32/IS*100,Transformation.CENTER); var b2=d2.visibleBounds; d2.translate(L0+1110-b2[0],T0-416-b2[1]);
}
var dir="/Users/maedanatsuki/Developer/ikaring-archive/assets/brand/logo/";
doc.saveAs(new File(dir+"SQuidLite3_logo_draft6_SQL3.ai"),new IllustratorSaveOptions());
for(var k=0;k<4;k++){doc.artboards.setActiveArtboardIndex(k);var o=new ExportOptionsPNG24();o.artBoardClipping=true;o.horizontalScale=100;o.verticalScale=100;doc.exportFile(new File(dir+"draft6_"+doc.artboards[k].name+".png"),ExportType.PNG24,o);}
"ok";

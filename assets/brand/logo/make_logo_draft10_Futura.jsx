function rgb(h){var c=new RGBColor();c.red=parseInt(h.substr(1,2),16);c.green=parseInt(h.substr(3,2),16);c.blue=parseInt(h.substr(5,2),16);return c;}
var BLUE=rgb("#2A0063"),GOLD=rgb("#BD9A45"),GOLD_D=rgb("#9A7A2E"),MILK=rgb("#FFFFF5"),PAPER=rgb("#FBF5EC");
var FONT="FuturaPT-Bold", AW=1500, AH=560, GAP=80;
var doc=app.documents.add(DocumentColorSpace.RGB,AW,AH);
var NAMES=["A_FuturaのQ","B_イカのQ","C_丸まる一本足","D_墨のしずく"], VAR=["plain","squid","curl","drop"];
doc.artboards[0].artboardRect=[0,AH,AW,0];doc.artboards[0].name=NAMES[0];
for(var i=1;i<4;i++){var y0=-i*(AH+GAP);var a=doc.artboards.add([0,y0+AH,AW,y0]);a.name=NAMES[i];}
function mkDot(g,cx,cy,r,col){var e=g.pathItems.ellipse(cy+r,cx-r,2*r,2*r);e.filled=true;e.fillColor=col;e.stroked=false;return e;}
function crSample(c,n){var out=[];for(var i=0;i<c.length-1;i++){var p0=c[Math.max(0,i-1)],p1=c[i],p2=c[i+1],p3=c[Math.min(c.length-1,i+2)];
  for(var k=0;k<n;k++){var t=k/n,t2=t*t,t3=t2*t;out.push([0.5*((2*p1[0])+(-p0[0]+p2[0])*t+(2*p0[0]-5*p1[0]+4*p2[0]-p3[0])*t2+(-p0[0]+3*p1[0]-3*p2[0]+p3[0])*t3),
    0.5*((2*p1[1])+(-p0[1]+p2[1])*t+(2*p0[1]-5*p1[1]+4*p2[1]-p3[1])*t2+(-p0[1]+3*p1[1]-3*p2[1]+p3[1])*t3)]);}}
  out.push(c[c.length-1]);return out;}
function taper(g,ctrl,w0,w1,col){var P=crSample(ctrl,16),N=P.length,L=[],R=[];
  for(var i=0;i<N;i++){var a=P[Math.max(0,i-1)],b=P[Math.min(N-1,i+1)];var tx=b[0]-a[0],ty=b[1]-a[1],l=Math.sqrt(tx*tx+ty*ty)||1;var nx=-ty/l,ny=tx/l;var w=(w0+(w1-w0)*Math.pow(i/(N-1),0.9))/2;
    L.push([P[i][0]+nx*w,P[i][1]+ny*w]);R.push([P[i][0]-nx*w,P[i][1]-ny*w]);}
  var p=g.pathItems.add();p.setEntirePath(L.concat(R.reverse()));p.closed=true;p.filled=true;p.fillColor=col;p.stroked=false;var e=P[N-1];mkDot(g,e[0],e[1],w1/2,col);return p;}
// しずく: 丸い底の中心 (cx,cy)、半径 r、先端の向き ang（度。90 で上）
function drop(g,cx,cy,r,ang,col){var k=0.5523*r;var D=[[0,2.3*r,0,2.3*r,0,2.3*r],[r,0,r,0.9*r,r,-k],[0,-r,k,-r,-k,-r],[-r,0,-r,-k,-r,0.9*r]];
  var th=(ang-90)*Math.PI/180,c=Math.cos(th),s=Math.sin(th);function T(x,y){return [cx+x*c-y*s,cy+x*s+y*c];}
  var p=g.pathItems.add();for(var i=0;i<D.length;i++){var a=D[i];var q=p.pathPoints.add();q.anchor=T(a[0],a[1]);q.leftDirection=T(a[2],a[3]);q.rightDirection=T(a[4],a[5]);}
  p.closed=true;p.filled=true;p.fillColor=col;p.stroked=false;return p;}
function txt(parent,str,col){var t=parent.textFrames.add();t.contents=str;var a=t.textRange.characterAttributes;a.size=200;a.textFont=app.textFonts.getByName(FONT);a.fillColor=col;return t;}
function bounds(it){return it.geometricBounds;} // [l,t,r,b]
function buildMark(parent,sc,v){
  var g=parent.groupItems.add();
  // 上段の幅は S と Q の丸（＝O）で測る。Q の尾は幅に入れない。
  var t1=txt(g,"SO",sc.fg); var t2=txt(g,"L3",sc.fg); t2.textRange.characters[1].characterAttributes.fillColor=sc.accent;
  var g1=t1.createOutline(), g2=t2.createOutline();
  var b1=bounds(g1), cap=b1[1]-b1[3]; var b2=bounds(g2);
  var topMid=(b1[0]+b1[2])/2, botMid=(b2[0]+b2[2])/2;
  g2.translate(topMid-botMid, (b1[3]-cap*0.16)-b2[1]);
  if(v=="plain"){ // 実際の Q の字形に差し替える（同じ位置・同じ大きさ）
    var tq=txt(g,"SQ",sc.fg); var gq=tq.createOutline(); var bq=bounds(gq); gq.translate(b1[0]-bq[0], b1[3]-bq[3]); g1.remove(); g1=gq;
  }
  if(v!="plain"){
    // O を探す（右側の字）
    var O=null,best=-1e9;for(var i=0;i<g1.pageItems.length;i++){var it=g1.pageItems[i];var bb=it.geometricBounds;if(bb[0]>best){best=bb[0];O=it;}}
    var ob=O.geometricBounds, cx=(ob[0]+ob[2])/2, cy=(ob[1]+ob[3])/2, Ro=(ob[2]-ob[0])/2, Ri=Ro*0.6;
    if(O.typename=="CompoundPathItem"){for(var j=0;j<O.pathItems.length;j++){var pb=O.pathItems[j].geometricBounds;var r=(pb[2]-pb[0])/2;if(r<Ro*0.98)Ri=r;}}
    var rc=(Ro+Ri)/2, th=Ro-Ri, k=rc/58;
    function M(pts){var r=[];for(var i=0;i<pts.length;i++)r.push([cx+(pts[i][0]-183)*k, cy+(pts[i][1]-243)*k]);return r;}
    if(v=="squid"){
      taper(g,M([[222,202],[250,166],[262,112],[258,62],[244,42],[232,48]]),th*0.80,th*0.2,sc.fg);
      taper(g,M([[234,218],[270,184],[286,132],[286,98],[276,84]]),th*0.62,th*0.17,sc.fg);
      if(sc.eyeWhite){mkDot(g,cx-0.3*Ri,cy+0.12*Ri,0.27*Ri,sc.eyeWhite);mkDot(g,cx+0.3*Ri,cy+0.12*Ri,0.27*Ri,sc.eyeWhite);mkDot(g,cx-0.27*Ri,cy+0.09*Ri,0.13*Ri,sc.pupil);mkDot(g,cx+0.33*Ri,cy+0.09*Ri,0.13*Ri,sc.pupil);}
      else{mkDot(g,cx-0.3*Ri,cy+0.12*Ri,0.2*Ri,sc.fg);mkDot(g,cx+0.3*Ri,cy+0.12*Ri,0.2*Ri,sc.fg);}
    } else if(v=="curl"){
      taper(g,M([[214.6,194.4],[246,178],[270,150],[276,118],[264,96],[246,98],[242,114],[252,122]]),th*0.88,th*0.26,sc.fg);
      var dp=M([[262,50]])[0]; drop(g,dp[0],dp[1],10*k,90,sc.accent);
    } else if(v=="drop"){
      var a=-45*Math.PI/180, dr=0.62*th; var dc=[cx+(Ro+dr*0.55)*Math.cos(a), cy+(Ro+dr*0.55)*Math.sin(a)];
      drop(g,dc[0],dc[1],dr,135,sc.accent);
    }
  }
  g2.zOrder(ZOrderMethod.BRINGTOFRONT); // 重なるところは L3 を手前に（足は 3 の後ろからのぞく）
  return g;}
function word(parent,str,size,sc){var t=parent.textFrames.add();t.contents=str;var a=t.textRange.characterAttributes;a.size=size;a.textFont=app.textFonts.getByName(FONT);a.fillColor=sc.fg;t.textRange.characters[str.length-1].characterAttributes.fillColor=sc.accent;return t.createOutline();}
function fitInto(item,left,top,w,h){var vb=item.visibleBounds;var iw=vb[2]-vb[0],ih=vb[1]-vb[3];var f=Math.min(w/iw,h/ih)*100;item.resize(f,f,true,true,true,true,f,Transformation.CENTER);vb=item.visibleBounds;iw=vb[2]-vb[0];ih=vb[1]-vb[3];item.translate(left+(w-iw)/2-vb[0],top-(h-ih)/2-vb[1]);}
var ON_PAPER={fg:BLUE,accent:GOLD_D,bg:PAPER}, ON_BLUE={fg:MILK,accent:GOLD,bg:BLUE,eyeWhite:MILK,pupil:BLUE};
for(var k2=0;k2<4;k2++){
  var r=doc.artboards[k2].artboardRect,L0=r[0],T0=r[1],v=VAR[k2];
  var bg=doc.pathItems.rectangle(T0,L0,AW,AH);bg.filled=true;bg.fillColor=PAPER;bg.stroked=false;
  var m=buildMark(doc,ON_PAPER,v); fitInto(m,L0+50,T0-50,330,460);
  var g2=doc.groupItems.add(); var m2=buildMark(g2,ON_PAPER,v); fitInto(m2,L0+440,T0-175,150,190);
  var w=word(g2,"SQuidLite3",62,ON_PAPER); var wb=w.visibleBounds; w.translate(L0+612-wb[0],T0-238-wb[1]);
  var ix=L0+1030,iy=T0-60,IS=300; var gi=doc.groupItems.add(); var sq=gi.pathItems.roundedRectangle(iy,ix,IS,IS,66,66);sq.filled=true;sq.fillColor=BLUE;sq.stroked=false;
  var mi=buildMark(gi,ON_BLUE,v); fitInto(mi,ix+IS*0.17,iy-IS*0.15,IS*0.66,IS*0.70);
  var d1=gi.duplicate(); d1.resize(64/IS*100,64/IS*100,true,true,true,true,64/IS*100,Transformation.CENTER); var b1=d1.visibleBounds; d1.translate(L0+1030-b1[0],T0-400-b1[1]);
  var d2=gi.duplicate(); d2.resize(32/IS*100,32/IS*100,true,true,true,true,32/IS*100,Transformation.CENTER); var b2=d2.visibleBounds; d2.translate(L0+1110-b2[0],T0-416-b2[1]);
}
var dir="/Users/maedanatsuki/Developer/ikaring-archive/assets/brand/logo/";
doc.saveAs(new File(dir+"SQuidLite3_logo_draft10_Futura.ai"),new IllustratorSaveOptions());
for(var k3=0;k3<4;k3++){doc.artboards.setActiveArtboardIndex(k3);var o=new ExportOptionsPNG24();o.artBoardClipping=true;o.horizontalScale=100;o.verticalScale=100;doc.exportFile(new File(dir+"draft10_"+doc.artboards[k3].name+".png"),ExportType.PNG24,o);}
"ok";

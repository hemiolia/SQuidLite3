function rgb(h){var c=new RGBColor();c.red=parseInt(h.substr(1,2),16);c.green=parseInt(h.substr(3,2),16);c.blue=parseInt(h.substr(5,2),16);return c;}
var BLUE=rgb("#2A0063"),BLUE2=rgb("#4B2A9A"),BLUE3=rgb("#6E4FC0"),GOLD=rgb("#BD9A45"),MILK=rgb("#FFFFF5"),PAPER=rgb("#FBF5EC");
var W=720,H=440,GAP=80;
var doc=app.documents.add(DocumentColorSpace.RGB,W,H);
var abs=doc.artboards; abs[0].artboardRect=[0,H,W,0]; abs[0].name="A3_墨だまりのデータベース";
var names=["","B3_イカのQ","C3_データのイカ","D3_アプリのアイコン"];
for(var i=1;i<4;i++){var x=i*(W+GAP);var a=abs.add([x,H,x+W,0]);a.name=names[i];}
function back(p){p.zOrder(ZOrderMethod.SENDTOBACK);return p;}
function fill(p,c){p.filled=true;p.fillColor=c;p.stroked=false;return p;}
function ell(cx,cy,w,h,c){return fill(doc.pathItems.ellipse(cy+h/2,cx-w/2,w,h),c);}
function rect(l,t,w,h,c){return fill(doc.pathItems.rectangle(t,l,w,h),c);}
function rrect(l,t,w,h,r,c){return fill(doc.pathItems.roundedRectangle(t,l,w,h,r,r),c);}
// pts: [[x,y,lx,ly,rx,ry],...] 方向点つきの閉じたパス
function bez(pts,c){var p=doc.pathItems.add();for(var i=0;i<pts.length;i++){var q=p.pathPoints.add();var a=pts[i];q.anchor=[a[0],a[1]];q.leftDirection=[a[2],a[3]];q.rightDirection=[a[4],a[5]];q.pointType=PointType.SMOOTH;}p.closed=true;return fill(p,c);}
// しずく（先端が上、丸い底の中心 cx,cy、半径 r）
function drop(cx,cy,r,c){var k=0.5523*r;return bez([[cx,cy+2.3*r,cx,cy+2.3*r,cx,cy+2.3*r],[cx+r,cy,cx+r,cy+0.9*r,cx+r,cy-k],[cx,cy-r,cx+k,cy-r,cx-k,cy-r],[cx-r,cy,cx-r,cy-k,cx-r,cy+0.9*r]],c);}
// 細る足（上端の中心 x,y、幅 w、長さ len、曲がり bend）
function tentacle(x,y,w,len,bend,c){var b=y-len;return bez([[x-w/2,y,x-w/2,y,x-w/2,y-len*0.5],[x+bend,b,x+bend-w*0.6,b,x+bend+w*0.6,b],[x+w/2,y,x+w/2,y-len*0.5,x+w/2,y]],c);}
function word(s,size,col){var t=doc.textFrames.add();t.contents=s;var a=t.textRange.characterAttributes;a.size=size;a.textFont=app.textFonts.getByName("FuturaPT-Bold");a.fillColor=col||BLUE;t.textRange.characters[s.length-1].characterAttributes.fillColor=GOLD;return t.createOutline();}
function place(g,cx,cy){g.left=cx-g.width/2;g.top=cy+g.height/2;}
function placeLeft(g,left,cy){g.left=left;g.top=cy+g.height/2;}
function group(items){var g=doc.groupItems.add();for(var i=items.length-1;i>=0;i--)items[i].move(g,ElementPlacement.PLACEATBEGINNING);return g;}
var PAPERS=[];for(var k=0;k<4;k++){var r=abs[k].artboardRect;PAPERS.push(rect(r[0],r[1],W,H,PAPER));}
var CY=H/2;

// A2: 円筒の底からイカの足のように墨が垂れる
(function(){var ox=0,cx=ox+175,w=170,h=42,topY=CY+110,body=150;var it=[];
 it.push(rect(cx-w/2,topY,w,body,BLUE)); it.push(ell(cx,topY-body,w,h,BLUE));
 // 段の線（胴の幅に収める細い楕円弧＝乳白の帯）
 for(var j=1;j<=2;j++){var y=topY-j*50;var s=doc.pathItems.ellipse(y+h/2,cx-w/2+3,w-6,h);s.filled=false;s.stroked=true;s.strokeColor=MILK;s.strokeWidth=4;it.push(s);var m=rect(cx-w/2,y+h/2+2,w,h/2+2,BLUE);it.push(m);}
 it.push(ell(cx,topY,w,h,BLUE2)); it.push(ell(cx,topY+2,w-30,h-14,BLUE3));
 // 3本の足（細って曲がる）と、先の墨のしずく
 var xs=[cx-50,cx,cx+50],bends=[-16,0,16],lens=[72,96,72];
 for(var j=0;j<3;j++){back(tentacle(xs[j],topY-body-h/2+20,30,lens[j]+8,bends[j],BLUE));}
 it.push(drop(cx+bends[1],topY-body-h/2+12-lens[1]-18,10,GOLD));
 var g=word("SQuidLite3",62); placeLeft(g,ox+305,CY);
})();

// B2: イカのQ（虫眼鏡の連想を残し、尾を曲がる足に）
(function(){var ox=W+GAP,cx=ox+160,cy=CY+20,R=110,t=38;
 ell(cx,cy,2*R,2*R,BLUE); ell(cx,cy,2*R-2*t,2*R-2*t,PAPER);
 ell(cx-26,cy+10,34,34,MILK); ell(cx-22,cy+6,18,18,BLUE);
 ell(cx+26,cy+10,34,34,MILK); ell(cx+30,cy+6,18,18,BLUE);
 var bx=cx+R*0.62,by=cy-R*0.62;
 var ts=[tentacle(bx-14,by+18,30,96,40,BLUE),tentacle(bx+8,by+8,30,104,58,GOLD),tentacle(bx+30,by-2,30,90,72,BLUE)];for(var q=0;q<3;q++){back(ts[q]);}
 var g=word("SQuidLite3",56); placeLeft(g,ox+330,CY);
})();

// C2: データのイカ（尖った頭とひれ、3層の帯、足）
(function(){var ox=2*(W+GAP),cx=ox+175,top=CY+150,w=130,body=180;var it=[];
 // ひれ
 bez([[cx,top-10,cx,top-10,cx,top-10],[cx+w*0.95,top-70,cx+w*0.8,top-40,cx+w*0.9,top-80],[cx+w*0.45,top-95,cx+w*0.6,top-95,cx+w*0.45,top-95]],BLUE2);
 bez([[cx,top-10,cx,top-10,cx,top-10],[cx-w*0.45,top-95,cx-w*0.45,top-95,cx-w*0.6,top-95],[cx-w*0.95,top-70,cx-w*0.9,top-80,cx-w*0.8,top-40]],BLUE2);
 // 胴（先が尖り、下が丸い）
 bez([[cx,top,cx,top,cx,top],[cx+w/2,top-body*0.55,cx+w*0.42,top-body*0.25,cx+w/2,top-body*0.8],[cx,top-body,cx+w*0.3,top-body,cx-w*0.3,top-body],[cx-w/2,top-body*0.55,cx-w/2,top-body*0.8,cx-w*0.42,top-body*0.25]],BLUE);
 // 3層を示す乳白の細帯2本
 for(var j=1;j<=2;j++){var y=top-body*0.32-j*34;var s=doc.pathItems.add();s.setEntirePath([[cx-w*0.4,y],[cx+w*0.4,y]]);s.filled=false;s.stroked=true;s.strokeColor=MILK;s.strokeWidth=5;s.strokeCap=StrokeCap.ROUNDENDCAP;}
 // 目
 ell(cx-26,top-body*0.82,30,30,MILK);ell(cx-23,top-body*0.84,15,15,BLUE);
 ell(cx+26,top-body*0.82,30,30,MILK);ell(cx+29,top-body*0.84,15,15,BLUE);
 // 足（5本、中央は金）
 var xs=[-44,-22,0,22,44],bend=[-14,-6,0,6,14];
 for(var j=0;j<5;j++){back(tentacle(cx+xs[j],top-body+24,18,j==2?88:74,bend[j],j==2?GOLD:BLUE));}
 var g=word("SQuidLite3",60); placeLeft(g,ox+300,CY);
})();

// D2: アプリのアイコン
(function(){var ox=3*(W+GAP),s=360,cx=ox+W/2,cy=CY;
 rrect(cx-s/2,cy+s/2,s,s,82,BLUE);
 var g=word("SQ3",150,MILK); place(g,cx-6,cy-6);
 g.pageItems; // 3 は金
 drop(cx+s/2-70,cy+s/2-92,13,GOLD);
})();

for(var k=0;k<4;k++){}
for(var k=0;k<PAPERS.length;k++){PAPERS[k].zOrder(ZOrderMethod.SENDTOBACK);}
var dir="/Users/maedanatsuki/Developer/ikaring-archive/assets/brand/logo/";
doc.saveAs(new File(dir+"SQuidLite3_logo_draft3.ai"),new IllustratorSaveOptions());
for(var k=0;k<4;k++){doc.artboards.setActiveArtboardIndex(k);var o=new ExportOptionsPNG24();o.artBoardClipping=true;o.horizontalScale=200;o.verticalScale=200;doc.exportFile(new File(dir+"draft3_"+doc.artboards[k].name+".png"),ExportType.PNG24,o);}
"ok";

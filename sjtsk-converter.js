// source: http://martin.hinner.info/geo/

function jtsk_to_wgs(X,Y,H)
{
  var coord = {wgs84_latitude:"", wgs84_longitude:"", lat: 0, lon: 0, vyska: 0};
 
  /* Přepočet vstupích údajů - vychazi z nejakeho skriptu, ktery jsem nasel na Internetu - nejsem autorem prepoctu. */

  /*Vypocet zemepisnych souradnic z rovinnych souradnic*/
  a=6377397.15508; 
  e=0.081696831215303;
  n=0.97992470462083; 
  konst_u_ro=12310230.12797036;
  sinUQ=0.863499969506341; 
  cosUQ=0.504348889819882;
  sinVQ=0.420215144586493; 
  cosVQ=0.907424504992097;
  alfa=1.000597498371542; 
  k=1.003419163966575;
  ro=Math.sqrt(X*X+Y*Y);
  epsilon=2*Math.atan(Y/(ro+X));
  D=epsilon/n; 
  S=2*Math.atan(Math.exp(1/n*Math.log(konst_u_ro/ro)))-Math.PI/2;
  sinS=Math.sin(S);
  cosS=Math.cos(S);
  sinU=sinUQ*sinS-cosUQ*cosS*Math.cos(D);
  cosU=Math.sqrt(1-sinU*sinU);
  sinDV=Math.sin(D)*cosS/cosU; 
  cosDV=Math.sqrt(1-sinDV*sinDV);
  sinV=sinVQ*cosDV-cosVQ*sinDV; 
  cosV=cosVQ*cosDV+sinVQ*sinDV;
  Ljtsk=2*Math.atan(sinV/(1+cosV))/alfa;
  t=Math.exp(2/alfa*Math.log((1+sinU)/cosU/k));
  pom=(t-1)/(t+1);
  do {
   sinB=pom;
   pom=t*Math.exp(e*Math.log((1+e*sinB)/(1-e*sinB))); 
   pom=(pom-1)/(pom+1);
  } while (Math.abs(pom-sinB)>1e-15);

   Bjtsk=Math.atan(pom/Math.sqrt(1-pom*pom));


/* Pravoúhlé souřadnice ve S-JTSK */   
   a=6377397.15508; f_1=299.152812853;
   e2=1-(1-1/f_1)*(1-1/f_1); ro=a/Math.sqrt(1-e2*Math.sin(Bjtsk)*Math.sin(Bjtsk));
   x=(ro+H)*Math.cos(Bjtsk)*Math.cos(Ljtsk);  
   y=(ro+H)*Math.cos(Bjtsk)*Math.sin(Ljtsk);  
   z=((1-e2)*ro+H)*Math.sin(Bjtsk);

/* Pravoúhlé souřadnice v WGS-84*/
   dx=570.69; dy=85.69; dz=462.84; 
   wz=-5.2611/3600*Math.PI/180;wy=-1.58676/3600*Math.PI/180;wx=-4.99821/3600*Math.PI/180; m=3.543e-6; 
   xn=dx+(1+m)*(x+wz*y-wy*z); yn=dy+(1+m)*(-wz*x+y+wx*z); zn=dz+(1+m)*(wy*x-wx*y+z);

/* Geodetické souřadnice v systému WGS-84*/
   a=6378137.0; f_1=298.257223563;
   a_b=f_1/(f_1-1); p=Math.sqrt(xn*xn+yn*yn); e2=1-(1-1/f_1)*(1-1/f_1);
   theta=Math.atan(zn*a_b/p); st=Math.sin(theta); ct=Math.cos(theta);
   t=(zn+e2*a_b*a*st*st*st)/(p-e2*a*ct*ct*ct);
   B=Math.atan(t);  L=2*Math.atan(yn/(p+xn));  H=Math.sqrt(1+t*t)*(p-a/Math.sqrt(1+(1-e2)*t*t));

/* Formát výstupních hodnot */   

   B=B/Math.PI*180;  

   coord.lat = B;
   latitude="N"; if (B<0) {B=-B; latitude="S";};

   st_sirky=Math.floor(B);  B=(B-st_sirky)*60; min_sirky=Math.floor(B);
   B=(B-min_sirky)*60; vt_sirky=Math.round(B*1000)/1000;
   latitude=st_sirky+"°"+min_sirky+"'"+vt_sirky+latitude;
   coord.wgs84_latitude = latitude;

   L=L/Math.PI*180;  
   coord.lon = L;
   longitude="E"; if (L<0) {L=-L; longitude="W";};

   st_delky=Math.floor(L);  L=(L-st_delky)*60; min_delky=Math.floor(L);
   L=(L-min_delky)*60; vt_delky=Math.round(L*1000)/1000;
   longitude=st_delky+"°"+min_delky+"'"+vt_delky+longitude;
   coord.wgs84_longitude = longitude;

   coord.vyska=Math.round((H)*100)/100;

  return coord;
}

module.exports = {
    convert: jtsk_to_wgs
}
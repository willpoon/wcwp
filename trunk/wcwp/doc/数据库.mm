<map version="0.8.1">
<!-- To view this file, download free mind mapping software FreeMind from http://freemind.sourceforge.net -->
<node COLOR="#338800" CREATED="1293760696417" ID="Freemind_Link_1624608527" MODIFIED="1293760712211" TEXT="&#x6570;&#x636e;&#x5e93;">
<node CREATED="1293760713125" ID="_" MODIFIED="1293760717495" POSITION="right" TEXT="&#x4ea7;&#x54c1;">
<node CREATED="1293760718468" FOLDED="true" ID="Freemind_Link_1197971043" MODIFIED="1293760721154" TEXT="oracle">
<node CREATED="1293760722049" ID="Freemind_Link_149758375" MODIFIED="1293760728712" TEXT="&#x6027;&#x80fd;">
<node CREATED="1293760729841" ID="Freemind_Link_268286021" MODIFIED="1293760733868" TEXT="&#x547d;&#x4e2d;&#x7387;">
<node CREATED="1293760744380" ID="Freemind_Link_315324135" MODIFIED="1293760745692" TEXT="http://www.51testing.com/?uid-19940-action-viewspace-itemid-1603"/>
</node>
<node CREATED="1294128563785" ID="Freemind_Link_1941713921" MODIFIED="1294128568502" TEXT="&#x7d22;&#x5f15;">
<node CREATED="1294128569568" ID="Freemind_Link_616572059" MODIFIED="1294128574726" TEXT="&#x51fd;&#x6570;&#x7d22;&#x5f15;"/>
</node>
</node>
</node>
</node>
<node CREATED="1294363043093" ID="Freemind_Link_1605896184" MODIFIED="1294363050144" POSITION="left" TEXT="db2">
<node CREATED="1294365108542" FOLDED="true" ID="Freemind_Link_1104472988" MODIFIED="1294365145680" TEXT="&#x5f00;&#x53d1;dev">
<node CREATED="1294365170958" ID="Freemind_Link_1181468247" MODIFIED="1294365170958" TEXT=""/>
</node>
<node CREATED="1294365119860" ID="Freemind_Link_640129222" MODIFIED="1294365149307" TEXT="&#x5e94;&#x7528;app">
<node CREATED="1294365166723" ID="Freemind_Link_1437443768" MODIFIED="1294365167832" TEXT="sql">
<node CREATED="1296012604224" ID="Freemind_Link_181425868" MODIFIED="1296012608654" TEXT="&#x6280;&#x5de7;">
<node CREATED="1296012609622" ID="Freemind_Link_263221190" MODIFIED="1296012626398" TEXT="&#x5e26;&#x81ea;&#x589e;&#x5217;&#x7684;&#x8868;">
<node CREATED="1296012659374" ID="Freemind_Link_1287952704" MODIFIED="1296012660393" TEXT="create table test(id integer not null generated always as identity(startwith 1,increment by 1,no cache),name varchar(20))  "/>
</node>
<node CREATED="1296117694703" ID="Freemind_Link_900247200" MODIFIED="1296117704886" TEXT="&#x5de6;&#x5173;&#x8054;">
<node CREATED="1296117684204" ID="Freemind_Link_1532376718" MODIFIED="1296117686841" TEXT="&#x5728;on&#x7684;&#x6761;&#x4ef6;&#x91cc;&#x9762;&#x5c3d;&#x91cf;&#x7684;&#x53ea;&#x5199;&#x5173;&#x8054;&#x6761;&#x4ef6;&#x548c;&#x5bf9;&#x5de6;&#x5173;&#x8054;&#x7684;&#x8868;&#x4f5c;&#x9650;&#x5236;&#xff0c;&#x800c;&#x5bf9;&#x4e3b;&#x8868;&#x7684;&#x9650;&#x5236;&#x4e0d;&#x8981;&#x5199;&#x5728;&#x8fd9;&#x91cc;&#x3002;&#x5982;&#x679c;&#x5199;&#x5728;&#x91cc;&#x9762;&#x7684;&#x8bdd;&#xff0c;&#x4e0d;&#x4f46;&#x901f;&#x5ea6;&#x975e;&#x5e38;&#x6162;&#xff0c;&#x800c;&#x4e14;&#x53ef;&#x80fd;&#x4f1a;&#x51fa;&#x73b0;&#x83ab;&#x540d;&#x5176;&#x5999;&#x7684;&#x7ed3;&#x679c;&#x3002;"/>
</node>
</node>
<node CREATED="1296115085265" ID="Freemind_Link_1212955630" MODIFIED="1296115087324" TEXT="ddl">
<node CREATED="1296115088274" ID="Freemind_Link_482598453" MODIFIED="1296115092719" TEXT="create table">
<node CREATED="1296115093812" ID="Freemind_Link_969253796" MODIFIED="1296115100218" TEXT="&#x5206;&#x533a;&#x952e;">
<node CREATED="1296115106573" FOLDED="true" ID="Freemind_Link_1637898123" MODIFIED="1296115111887" TEXT="&#x89c4;&#x5219;">
<node CREATED="1296115126615" ID="Freemind_Link_1329931140" MODIFIED="1296115128629" TEXT="The partitioning key is defined using the DISTRIBUTED BY HASH clause in the CREATE TABLE command. After the partition key is defined, it cannot be altered. The only way to change it is to recreate the table.  The following rules and recommendations apply to the partitioning key definition: The primary key and any unique index of the table must be a superset of the associated partitioning key. In other words, all columns that are part of the partitioning key must be present in the primary key or unique index definition. The order of the columns does not matter. A partitioning key should include one to three columns. Typically, the fewer the columns, the better. An integer partitioning key is more efficient than a character key, which is more efficient than a decimal key. If there is no partitioning key provided explicitly in the CREATE TABLE command, the following defaults are used:  If a primary key is specified in the CREATE TABLE statement, the first column of the primary key is used as the distribution key. If there is no primary key, the first column that is not a long field is used."/>
</node>
<node CREATED="1296115141430" ID="Freemind_Link_363802104" MODIFIED="1296115143553" TEXT="&#x4f5c;&#x7528;">
<node CREATED="1296115144464" ID="Freemind_Link_1152063011" MODIFIED="1296115145464" TEXT="Choosing the right partitioning key is critical for two reasons: It improves the performance of the queries that use hashed partition It balances the storage requirements for all partitions"/>
</node>
</node>
</node>
</node>
</node>
<node CREATED="1294367188189" FOLDED="true" ID="Freemind_Link_1582806372" MODIFIED="1294367194696" TEXT="&#x8fde;&#x63a5;">
<node CREATED="1294826321013" ID="Freemind_Link_1157468444" MODIFIED="1294826337565" TEXT="connect to dbname user xxx using xxx"/>
<node CREATED="1295839967325" FOLDED="true" ID="Freemind_Link_461519656" MODIFIED="1295839969740" TEXT="catalog">
<node CREATED="1295849193565" ID="Freemind_Link_979325282" MODIFIED="1295849195228" TEXT="&#x4f60;&#x53ef;&#x4ee5;&#x8fd9;&#x6837;&#x7406;&#x89e3;&#x7f16;&#x76ee;&#xff08;catalog)&#xff0c;&#x7f16;&#x76ee;&#x5c31;&#x662f;&#x5728;&#x672c;&#x5730;&#x6216;&#x8fdc;&#x7a0b;&#x5efa;&#x7acb;&#x5ba2;&#x6237;&#x7aef;&#x5230;&#x670d;&#x52a1;&#x5668;&#x7684;&#x6570;&#x636e;&#x5e93;&#x8fde;&#x63a5;&#x7684;&#x76ee;&#x7684;&#xff0c;&#x4ed6;&#x7c7b;&#x4f3c;Oracle&#x6570;&#x636e;&#x5e93;&#x4e2d;&#x7684;&#x901a;&#x8fc7;SQL*NET&#x6216;netca&#x914d;&#x7f6e;&#x5ba2;&#x6237;&#x7aef;&#x5230;&#x670d;&#x52a1;&#x5668;&#x7684;&#x8fde;&#x63a5;&#xff1b;&#x7c7b;&#x4f3c;SYBASE&#x4e2d;&#x7684;OPEN CLIENT&#xff1b;&#x7c7b;&#x4f3c;informix&#x4e2d;Iconnect"/>
<node CREATED="1295849239183" ID="Freemind_Link_1329289277" MODIFIED="1295850264054" TEXT="&#x65b9;&#x6cd5;">
<node CREATED="1295850265369" ID="Freemind_Link_1027922751" MODIFIED="1295850266389" TEXT="db2 catalog tcpip node p570 remote 172.10.10.10 server 50000 &#x4e0e; db2 catalog db mydb at node p570"/>
</node>
<node CREATED="1295850240573" ID="Freemind_Link_1784728219" MODIFIED="1295850245163" TEXT="&#x67e5;&#x770b;">
<node CREATED="1295850269259" ID="Freemind_Link_345509048" MODIFIED="1295850286617" TEXT="node">
<node CREATED="1295850291380" ID="Freemind_Link_988570708" MODIFIED="1295850299230" TEXT="db2 list node directory"/>
</node>
<node CREATED="1295850287913" ID="Freemind_Link_1026064616" MODIFIED="1295850288992" TEXT="db">
<node CREATED="1295850301200" ID="Freemind_Link_1471891596" MODIFIED="1295850307863" TEXT="db2 list db directory"/>
</node>
</node>
<node CREATED="1295850345951" ID="Freemind_Link_1134217700" MODIFIED="1295850353779" TEXT="&#x53cd;&#x7f16;&#x76ee;">
<node CREATED="1295850389312" ID="Freemind_Link_1681286737" MODIFIED="1295850393566" TEXT="node">
<node CREATED="1295850397657" ID="Freemind_Link_237926782" MODIFIED="1295850413524" TEXT="db2 uncatalog node nodename"/>
</node>
<node CREATED="1295850394846" ID="Freemind_Link_144371342" MODIFIED="1295850396305" TEXT="db">
<node CREATED="1295850415462" ID="Freemind_Link_1969218657" MODIFIED="1295850493302" TEXT="db2 uncatalog database dbname"/>
</node>
</node>
</node>
<node CREATED="1295855140541" FOLDED="true" ID="Freemind_Link_1525786022" MODIFIED="1295855144499" TEXT="&#x5e94;&#x7528;">
<node CREATED="1295855145422" FOLDED="true" ID="Freemind_Link_373004680" MODIFIED="1295855151045" TEXT="&#x7ba1;&#x7406;">
<node CREATED="1295855507181" ID="Freemind_Link_581406198" MODIFIED="1295855517116" TEXT="&#x6740;&#x8fdb;&#x7a0b;&#xff0c;&#x65ad;&#x8fde;&#x63a5;">
<node CREATED="1295855518139" ID="Freemind_Link_689746405" MODIFIED="1295855527378" TEXT="db2 force application .."/>
</node>
<node CREATED="1295857927364" ID="Freemind_Link_1147854149" MODIFIED="1295857931841" TEXT="db2bp">
<node CREATED="1295857932755" ID="Freemind_Link_624713493" MODIFIED="1295857934351" TEXT="The command line processor consists of two processes: the front-end process  (the DB2 command), which acts as the user interface, and the back-end  process (db2bp), which maintains a database connection."/>
</node>
</node>
</node>
</node>
</node>
<node CREATED="1294365125577" ID="Freemind_Link_136725535" MODIFIED="1294365154291" TEXT="&#x7ba1;&#x7406;admin">
<node CREATED="1294365235893" FOLDED="true" ID="Freemind_Link_1673721450" MODIFIED="1294365239811" TEXT="tools">
<node CREATED="1294365240799" ID="Freemind_Link_189895546" MODIFIED="1294365255512" TEXT="db2&#x547d;&#x4ee4;&#x5927;&#x5168;(V8&#x7248;&#x672c;).pdf"/>
</node>
<node CREATED="1294365294103" ID="Freemind_Link_907528734" MODIFIED="1294365302012" TEXT="&#x6027;&#x80fd;&#x7ef4;&#x62a4;">
<node CREATED="1294365302970" ID="Freemind_Link_974326143" MODIFIED="1294365314165" TEXT="&#x8868;&#x7a7a;&#x95f4;">
<node CREATED="1294365316183" ID="Freemind_Link_1834831200" MODIFIED="1294367293998" TEXT="&#x76f8;&#x5173;&#x7cfb;&#x7edf;&#x8868;"/>
<node CREATED="1294365334880" FOLDED="true" ID="Freemind_Link_17687707" MODIFIED="1294365337862" TEXT="&#x547d;&#x4ee4;">
<node CREATED="1294367448834" ID="Freemind_Link_116711975" MODIFIED="1294367458384" TEXT="db2 list tablespaces"/>
</node>
<node CREATED="1294367301289" ID="Freemind_Link_460375834" MODIFIED="1294367303647" TEXT="&#x72b6;&#x6001;">
<node CREATED="1294367765640" ID="Freemind_Link_1770784932" MODIFIED="1294367770757" TEXT="db2tbsts"/>
<node CREATED="1294989711394" ID="Freemind_Link_1399746155" MODIFIED="1294989719132" TEXT="&#x5229;&#x7528;&#x7387;">
<node CREATED="1294989740697" ID="Freemind_Link_141589680" MODIFIED="1294989759013" TEXT="sysibmadm.tbsp_utilization"/>
</node>
</node>
<node CREATED="1294367705582" ID="Freemind_Link_500556660" MODIFIED="1294367709575" TEXT="&#x8868;">
<node CREATED="1294367731858" ID="Freemind_Link_1523092805" MODIFIED="1294367735167" TEXT="&#x72b6;&#x6001;">
<node CREATED="1294367773251" ID="Freemind_Link_574509982" MODIFIED="1294367854927" TEXT="**pending"/>
<node CREATED="1294367856618" ID="Freemind_Link_776233002" MODIFIED="1294367859054" TEXT="lock">
<node CREATED="1294367860370" ID="Freemind_Link_182104859" MODIFIED="1294367865816" TEXT="&#x8bbf;&#x95ee;&#x63a7;&#x5236;"/>
</node>
</node>
<node CREATED="1294367945486" ID="Freemind_Link_623400466" MODIFIED="1294367949167" TEXT="&#x52a0;&#x8f7d;">
<node CREATED="1294367951529" ID="Freemind_Link_1705434236" MODIFIED="1294367964635" TEXT="&#x52a0;&#x8f7d;&#x67e5;&#x8be2;"/>
<node CREATED="1294367966497" ID="Freemind_Link_635505833" MODIFIED="1294367969166" TEXT="&#x72b6;&#x6001;"/>
</node>
</node>
<node CREATED="1294826393686" ID="Freemind_Link_297592330" MODIFIED="1294826400051" TEXT="&#x521b;&#x5efa;">
<node CREATED="1294826401352" ID="Freemind_Link_343985658" MODIFIED="1294826404864" TEXT="&#x53c2;&#x6570;"/>
</node>
<node CREATED="1294826430721" ID="Freemind_Link_1820778862" MODIFIED="1294826433466" TEXT="&#x6743;&#x9650;"/>
<node CREATED="1296116729724" ID="Freemind_Link_752923126" MODIFIED="1296116807851" TEXT="&#x5229;&#x7528;&#x60c5;&#x51b5;&#x4e0e;&#x6027;&#x80fd;">
<node CREATED="1296116738828" ID="Freemind_Link_495584394" MODIFIED="1296116835282" TEXT="&#x5206;&#x5e03;&#x5728;&#x5404;&#x4e2a;&#x8282;&#x70b9;&#x4e0a;&#x8981;&#x5747;&#x5300;">
<node CREATED="1296117026341" ID="Freemind_Link_924011185" MODIFIED="1296117027466" TEXT="SELECT DBPARTITIONNUM( column), COUNT(*) FROM table GROUP BY DBPARTITIONNUM( column)"/>
</node>
<node CREATED="1296116984216" ID="Freemind_Link_1356776862" MODIFIED="1296116985498" TEXT="2&#x3002;&#x5206;&#x533a;&#x952e;&#x7684;&#x9009;&#x62e9;&#x7684;&#x5173;&#x952e;&#x662f;&#x627e;&#x51fa;&#x5173;&#x8054;&#x5ea6;&#x6700;&#x9ad8;&#x7684;&#x952e;&#x7684;&#x7ec4;&#x5408;&#xff0c;&#x76ee;&#x6807;&#x662f;&#x51cf;&#x5c11;&#x6570;&#x636e;&#x5728;&#x5404;&#x5206;&#x533a;&#x4e4b;&#x95f4;&#x79fb;&#x52a8;&#x3002;&#x5176;&#x6b21;&#x662f;&#x8003;&#x8651;&#x80fd;&#x5426;&#x5e73;&#x5747;&#x5206;&#x5e03;&#x6570;&#x636e;&#x3002;&#x6700;&#x597d;&#x80fd;&#x4e24;&#x8005;&#x517c;&#x987e;&#x3002;&#x4f60;&#x8fd9;&#x79cd;&#x60c5;&#x51b5;&#x987b;&#x5148;&#x5206;&#x6790;&#x6700;&#x5e38;&#x7528;&#x7684;SQL&#x8bed;&#x53e5;&#xff0c;&#x627e;&#x51fa;&#x5404;&#x4e2a;table&#x7684;&#x5173;&#x8054;&#x952e;&#x503c;&#x518d;&#x4ee5;&#x6b64;&#x952e;&#x503c;&#xff08;&#x6216;&#x952e;&#x503c;&#x7ec4;&#x5408;&#xff09;&#x91cd;&#x65b0;&#x5206;&#x5e03;&#x6570;&#x636e;&#x3002;"/>
<node CREATED="1296117298583" ID="Freemind_Link_931799656" MODIFIED="1296117301159" TEXT="&#x7b56;&#x7565;">
<node CREATED="1296117329302" ID="Freemind_Link_127086453" MODIFIED="1296117357565" TEXT="&#x6839;&#x636e;&#x5b58;&#x50a8;&#x548c;&#x8bbf;&#x95ee;&#x7684;&#x9700;&#x8981;&#x548c;&#x6570;&#x636e;&#x7684;&#x7279;&#x70b9;">
<node CREATED="1296117302195" ID="Freemind_Link_937229618" MODIFIED="1296117309166" TEXT="&#x591a;&#x8282;&#x70b9;&#x8868;&#x7a7a;&#x95f4;"/>
<node CREATED="1296117311180" ID="Freemind_Link_254937692" MODIFIED="1296117318730" TEXT="&#x5355;&#x8282;&#x70b9;&#x8868;&#x7a7a;&#x95f4;"/>
</node>
</node>
</node>
</node>
<node CREATED="1294366372447" ID="Freemind_Link_342679617" MODIFIED="1294366392196" TEXT="&#x57fa;&#x7840;&#x670d;&#x52a1;">
<node CREATED="1294366405899" ID="Freemind_Link_1134327105" MODIFIED="1294366408553" TEXT="DAS">
<node CREATED="1294366416749" ID="Freemind_Link_1222847968" MODIFIED="1294366583656" TEXT="&#x4f5c;&#x7528;&#xff1a;&#x652f;&#x6491;instances,tools"/>
<node CREATED="1294366573960" ID="Freemind_Link_1035605631" MODIFIED="1294366577860" TEXT="&#x542f;&#x52a8;">
<node CREATED="1294366585861" ID="Freemind_Link_629002944" MODIFIED="1294366593663" TEXT="db2admin start"/>
</node>
<node CREATED="1294366792819" ID="Freemind_Link_403665271" MODIFIED="1294366805916" TEXT="&#x5b9e;&#x4f8b;instance">
<node CREATED="1294366807482" ID="Freemind_Link_1144533085" MODIFIED="1294366834425" TEXT="&#x4f5c;&#x7528;:&#x63d0;&#x4f9b;&#x5355;&#x72ec;&#x8fd0;&#x884c;&#x7684;&#x73af;&#x5883;"/>
<node CREATED="1294367118457" ID="Freemind_Link_1617011765" MODIFIED="1294367123810" TEXT="&#x5f00;&#x542f;&#x5173;&#x95ed;">
<node CREATED="1294367131974" ID="Freemind_Link_1145210590" MODIFIED="1294367134660" TEXT="db2start"/>
<node CREATED="1294367136615" ID="Freemind_Link_1457937280" MODIFIED="1294367138879" TEXT="db2stop"/>
</node>
</node>
</node>
</node>
<node CREATED="1294369887063" ID="Freemind_Link_1432729483" MODIFIED="1294369900590" TEXT="&#x64cd;&#x4f5c;&#x7cfb;&#x7edf;&#x76f8;&#x5173;">
<node CREATED="1294370219156" ID="Freemind_Link_1911652364" MODIFIED="1294370226604" TEXT="&#x6587;&#x4ef6;&#x7cfb;&#x7edf;&#x7a7a;&#x95f4;">
<node CREATED="1294369901563" ID="Freemind_Link_518040930" MODIFIED="1294369915980" TEXT="get db cfg"/>
<node CREATED="1294370195251" ID="Freemind_Link_1394396257" MODIFIED="1294370210321" TEXT="df -h"/>
<node CREATED="1294370249091" ID="Freemind_Link_977522223" MODIFIED="1294370253194" TEXT="&#x5b58;&#x50a8;&#x7ba1;&#x7406;">
<node CREATED="1294370253995" ID="Freemind_Link_1872116936" MODIFIED="1294370269948" TEXT="&#x7b2c;&#x4e09;&#x65b9;&#x5de5;&#x5177;tsm"/>
<node CREATED="1294370470533" ID="Freemind_Link_1682201643" MODIFIED="1294370476649" TEXT="&#x5907;&#x4efd;"/>
<node CREATED="1294370487372" ID="Freemind_Link_1098415748" MODIFIED="1294370490135" TEXT="&#x65e5;&#x5fd7;">
<node CREATED="1294370491045" ID="Freemind_Link_28597271" MODIFIED="1294370493356" TEXT="&#x5f52;&#x6863;"/>
<node CREATED="1295852440826" ID="Freemind_Link_768489375" MODIFIED="1295852449145" TEXT="&#x5206;&#x7c7b;">
<node CREATED="1295852451971" ID="Freemind_Link_200110030" MODIFIED="1295852455513" TEXT="&#x6570;&#x636e;&#x65e5;&#x5fd7;"/>
<node CREATED="1295852456952" ID="Freemind_Link_775677875" MODIFIED="1295852460649" TEXT="&#x7cfb;&#x7edf;&#x65e5;&#x5fd7;"/>
<node CREATED="1295852462297" ID="Freemind_Link_1232270915" MODIFIED="1295852466605" TEXT="&#x9519;&#x8bef;&#x65e5;&#x5fd7;">
<node CREATED="1295853170224" ID="Freemind_Link_1438610361" MODIFIED="1295853724027" TEXT="db2diag">
<node CREATED="1295854197307" ID="Freemind_Link_720292375" MODIFIED="1295854198579" TEXT="db2diag -time 2006-01-01 -node &quot;0,1,2&quot; -level &quot;Severe, Error&quot; "/>
<node CREATED="1295854343400" ID="Freemind_Link_1372679844" MODIFIED="1295854352610" TEXT="-h for help"/>
</node>
</node>
</node>
</node>
</node>
</node>
<node CREATED="1295855071050" ID="Freemind_Link_353423589" MODIFIED="1295855084030" TEXT="&#x5176;&#x5b83;&#x8d44;&#x6e90;">
<node CREATED="1295855085004" ID="Freemind_Link_1591704639" MODIFIED="1295855088970" TEXT="CPU"/>
<node CREATED="1295855090744" ID="Freemind_Link_797140634" MODIFIED="1295855096822" TEXT="MEMORY">
<node CREATED="1295855101425" ID="Freemind_Link_1574726737" MODIFIED="1295855108709" TEXT="&#x8d44;&#x6e90;&#x7ba1;&#x7406;">
<node CREATED="1295855109765" ID="Freemind_Link_328175175" MODIFIED="1295855112686" TEXT="&#x8fde;&#x63a5;">
<node CREATED="1295855116317" ID="Freemind_Link_548658833" MODIFIED="1295855124734" TEXT="&#x5e94;&#x7528;">
<node CREATED="1295855126128" ID="Freemind_Link_1561698373" MODIFIED="1295855129037" TEXT="&#x7ba1;&#x7406;"/>
</node>
</node>
</node>
</node>
</node>
</node>
<node CREATED="1296009741946" ID="Freemind_Link_1056945368" MODIFIED="1296009749065" TEXT="&#x5f71;&#x54cd;&#x56e0;&#x7d20;">
<node CREATED="1296009753597" ID="Freemind_Link_140970498" MODIFIED="1296009755876" TEXT="&#x78c1;&#x76d8;"/>
<node CREATED="1296009757114" ID="Freemind_Link_1866270198" MODIFIED="1296009762335" TEXT="&#x7f51;&#x7edc;"/>
<node CREATED="1296009763932" ID="Freemind_Link_1950439078" MODIFIED="1296009766071" TEXT="&#x5185;&#x5b58;"/>
<node CREATED="1296009772416" ID="Freemind_Link_943727738" MODIFIED="1296009776194" TEXT="cpu"/>
</node>
</node>
<node CREATED="1295853152846" ID="Freemind_Link_656327762" MODIFIED="1295853161647" TEXT="&#x707e;&#x96be;">
<node CREATED="1295853162874" ID="Freemind_Link_730308879" MODIFIED="1295853165255" TEXT="&#x9519;&#x8bef;">
<node CREATED="1295853166485" ID="Freemind_Link_1244631106" MODIFIED="1295853168884" TEXT="&#x65e5;&#x5fd7;"/>
</node>
</node>
<node CREATED="1296007426557" ID="Freemind_Link_522191854" MODIFIED="1296007436158" TEXT="&#x539f;&#x7406;">
<node CREATED="1296007457653" ID="Freemind_Link_1520852392" MODIFIED="1296007463186" TEXT="&#x6982;&#x5ff5;">
<node CREATED="1296116378606" ID="Freemind_Link_46122514" MODIFIED="1296116395230" TEXT="node/&#x4e3b;&#x673a;">
<node CREATED="1296007509855" ID="Freemind_Link_832479462" MODIFIED="1296007512673" TEXT="instance">
<node CREATED="1296007522634" ID="Freemind_Link_1413320768" MODIFIED="1296007525366" TEXT="database">
<node CREATED="1296007464720" ID="Freemind_Link_863238515" MODIFIED="1296007477054" TEXT="tablespace">
<node CREATED="1296007546575" ID="Freemind_Link_137009432" MODIFIED="1296007551452" TEXT="&#x903b;&#x8f91;"/>
</node>
<node CREATED="1296007479463" ID="Freemind_Link_1441481048" MODIFIED="1296007485588" TEXT="container">
<node CREATED="1296007553688" ID="Freemind_Link_1338069043" MODIFIED="1296007555625" TEXT="&#x7269;&#x7406;"/>
<node CREATED="1296008448959" ID="Freemind_Link_1387806357" MODIFIED="1296008451293" TEXT="&#x7c7b;&#x578b;">
<node CREATED="1296008452297" ID="Freemind_Link_1333722813" MODIFIED="1296008456090" TEXT="file"/>
<node CREATED="1296008458428" ID="Freemind_Link_1381963863" MODIFIED="1296008461386" TEXT="directory"/>
<node CREATED="1296008466814" ID="Freemind_Link_474722738" MODIFIED="1296008472456" TEXT="device"/>
</node>
</node>
</node>
<node CREATED="1296197675054" ID="Freemind_Link_1420485628" MODIFIED="1296197681481" TEXT="&#x521b;&#x5efa;">
<node CREATED="1296197682547" ID="Freemind_Link_480404901" MODIFIED="1296197685869" TEXT="db2icrt">
<node CREATED="1296198732469" ID="Freemind_Link_98467082" MODIFIED="1296198733628" TEXT="&#x5b89;&#x88c5;&#x65f6;&#xff0c;&#x5b89;&#x88c5;&#x7a0b;&#x5e8f;&#x4f1a;&#x8bbe;&#x7f6e;&#x4e00;&#x4e9b;&#x76f8;&#x5e94;&#x7684;DB2&#x73af;&#x5883;&#x53d8;&#x91cf;&#x3002;"/>
</node>
<node CREATED="1296197688837" ID="Freemind_Link_217936853" MODIFIED="1296197719995" TEXT="set db2instance=instancename"/>
</node>
<node CREATED="1296198192272" ID="Freemind_Link_182992018" MODIFIED="1296198198984" TEXT="&#x5220;&#x9664;">
<node CREATED="1296198200419" ID="Freemind_Link_16891748" MODIFIED="1296198203089" TEXT="db2idrop">
<node CREATED="1296198237577" ID="Freemind_Link_902133570" MODIFIED="1296198239267" TEXT="&#x6570;&#x636e;&#x5e93;&#x5b9e;&#x4f8b;&#x5220;&#x9664;&#x4e0d;&#x8981;&#x7d27;&#xff0c;&#x54e5;&#x4eec;&#x513f;&#xff0c;&#x4f60;&#x7684;&#x6570;&#x636e;&#x5e93;&#x6587;&#x4ef6;&#x8fd8;&#x5728;&#x4e0d;&#x5728;&#xff0c;&#x5728;&#x7684;&#x8bdd;&#x91cd;&#x65b0;&#x6ce8;&#x518c;&#x4e0b;&#x5c31;&#x53ef;&#x4ee5;&#x5566;&#x3002;"/>
</node>
<node CREATED="1296198310658" ID="Freemind_Link_254219323" MODIFIED="1296198311996" TEXT="db2 catalog db xxx on ddd ddd&#x8bbe;&#x7f6e;&#x4e3a;&#x6570;&#x636e;&#x5e93;&#x6587;&#x4ef6;&#x76ee;&#x5f55;&#x5373;&#x53ef;&#x3002;&#x3002;&#x3002;"/>
</node>
<node CREATED="1296198457023" ID="Freemind_Link_4764785" MODIFIED="1296198466581" TEXT="what is ?">
<node CREATED="1296198467644" ID="Freemind_Link_1732924576" MODIFIED="1296198485463" TEXT="DB2&#x5b9e;&#x4f8b;&#x662f;&#x4e00;&#x4e2a;&#x903b;&#x8f91;&#x7684;&#x6570;&#x636e;&#x5e93;&#x670d;&#x52a1;&#x5668;&#x73af;&#x5883;&#x3002;DB2&#x6570;&#x636e;&#x5e93;&#x5c31;&#x5c06;&#x521b;&#x5efa;&#x5728;&#x6570;&#x636e;&#x5e93;&#x670d;&#x52a1;&#x5668;&#x7684;DB2&#x5b9e;&#x4f8b;&#x4e0a;&#x9762;&#x3002;"/>
</node>
<node CREATED="1296198535472" ID="Freemind_Link_815403140" MODIFIED="1296198539910" TEXT="&#x4f5c;&#x7528;">
<node CREATED="1296198543046" ID="Freemind_Link_747554163" MODIFIED="1296198544220" TEXT="&#x6bcf;&#x4e2a;&#x5b9e;&#x4f8b;&#x90fd;&#x53ef;&#x4ee5;&#x5355;&#x72ec;&#x4f5c;&#x4e3a;&#x4e00;&#x4e2a;DB2&#x670d;&#x52a1;&#x5668;&#x5bf9;&#x5916;&#x63d0;&#x4f9b;&#x670d;&#x52a1;&#x3002;">
<node CREATED="1296198581924" ID="Freemind_Link_298795689" MODIFIED="1296198586288" TEXT="&#x4f8b;&#x5982;">
<node CREATED="1296198587214" ID="Freemind_Link_484635602" MODIFIED="1296198588031" TEXT="&#x7528;&#x6237;&#x53ef;&#x4ee5;&#x540c;&#x65f6;&#x5728;&#x4e00;&#x53f0;&#x673a;&#x5668;&#x4e0a;&#x521b;&#x5efa;&#x62e5;&#x6709;&#x76f8;&#x540c;&#x4ee3;&#x7801;&#x8def;&#x5f84;&#x7684;&#x6d4b;&#x8bd5;&#x73af;&#x5883;&#x548c;&#x4ea7;&#x54c1;&#x73af;&#x5883;"/>
</node>
</node>
<node CREATED="1296198545783" ID="Freemind_Link_1550085319" MODIFIED="1296198554898" TEXT="&#x8f7d;&#x4f53;&#x7684;&#x4f5c;&#x7528;"/>
</node>
</node>
</node>
</node>
</node>
</node>
<node CREATED="1294367431123" ID="Freemind_Link_705182453" MODIFIED="1294367439110" TEXT="&#x9057;&#x7559;&#x95ee;&#x9898;">
<node CREATED="1294367440317" ID="Freemind_Link_919758818" MODIFIED="1294367443236" TEXT="node"/>
<node CREATED="1294367510644" ID="Freemind_Link_561179449" MODIFIED="1294367513748" TEXT="pages"/>
<node CREATED="1294367517997" ID="Freemind_Link_816959768" MODIFIED="1294367522726" TEXT="extent"/>
<node CREATED="1294367652423" ID="Freemind_Link_116461483" MODIFIED="1294367656465" TEXT="locks"/>
<node CREATED="1294368044249" ID="Freemind_Link_1719229480" MODIFIED="1294368047962" TEXT="sqlstate"/>
<node CREATED="1295855017818" FOLDED="true" ID="Freemind_Link_238304433" MODIFIED="1295855021188" TEXT="agent">
<node CREATED="1295855022224" ID="Freemind_Link_884410772" MODIFIED="1295855023896" TEXT="&#x4ee3;&#x7406;&#x7a0b;&#x5e8f;&#x53ef;&#x88ab;&#x8ba4;&#x4e3a;&#x662f;&#x4e00;&#x4e2a;&#x4ee3;&#x8868;&#x5e94;&#x7528;&#x7a0b;&#x5e8f;&#x6267;&#x884c;&#x6240;&#x6709;&#x6570;&#x636e;&#x5e93;&#x64cd;&#x4f5c;&#x7684;&#x201c;&#x5de5;&#x4f5c;&#x7a0b;&#x5e8f;&#x201d;"/>
</node>
<node CREATED="1295856048306" ID="Freemind_Link_1210841273" MODIFIED="1295856060094" TEXT="&#x524d;&#x6eda;&#xff0c;&#x56de;&#x6eda;"/>
</node>
</node>
</node>
</map>

import React, { useState, useEffect } from 'react';
import { 
  Search, Map as MapIcon, Zap, Database, Lock, 
  ArrowRight, Building2, Terminal, Activity, 
  Wifi, ServerCrash, MousePointer2, ChevronRight,
  LayoutGrid, List, PieChart, Bell, Settings, User,
  TrendingUp, AlertTriangle, FileText
} from 'lucide-react';

// --- CONFIGURATION ---
const API_URL = "http://localhost:8000";

// --- MOCK DATA FOR CHARTS (Placeholders until we build analytics API) ---
const RISK_FACTORS = [
  { name: 'Credit', score: 8.5, color: 'text-red-500', bar: 'bg-red-500' },
  { name: 'Market', score: 6.2, color: 'text-orange-500', bar: 'bg-orange-500' },
  { name: 'Liquidity', score: 7.8, color: 'text-red-400', bar: 'bg-red-400' },
  { name: 'Operational', score: 5.4, color: 'text-yellow-500', bar: 'bg-yellow-500' },
  { name: 'Legal', score: 8.1, color: 'text-red-500', bar: 'bg-red-500' },
];

// --- UI COMPONENTS ---

const Badge = ({ children, variant = 'neutral', className = '' }) => {
  const styles = {
    critical: "bg-red-500/10 text-red-500 border-red-500/30",
    high: "bg-orange-500/10 text-orange-500 border-orange-500/30",
    medium: "bg-yellow-500/10 text-yellow-500 border-yellow-500/30",
    low: "bg-emerald-500/10 text-emerald-500 border-emerald-500/30",
    neutral: "bg-zinc-800 text-zinc-400 border-zinc-700",
  };
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded-sm text-[10px] font-mono border ${styles[variant] || styles.neutral} tracking-wide uppercase ${className}`}>
      {children}
    </span>
  );
};

// The new card design based on 'Screen specifically for displa.html'
const DistressCard = ({ asset, onClick, isSelected }) => {
  // Determine Risk Logic
  let riskVariant = 'low';
  let riskScore = '2.4';
  
  // Logic: F/G = Critical, E = High, D = Medium
  if (asset.asset_rating_band === 'F' || asset.asset_rating_band === 'G') {
    riskVariant = 'critical';
    riskScore = '9.1';
  } else if (asset.asset_rating_band === 'E') {
    riskVariant = 'high';
    riskScore = '7.5';
  } else if (asset.asset_rating_band === 'D') {
    riskVariant = 'medium';
    riskScore = '5.2';
  }

  return (
    <button 
      onClick={onClick}
      className={`w-full text-left mb-2 group relative overflow-hidden transition-all duration-200 border-l-2
        ${isSelected 
          ? 'bg-zinc-900 border-l-emerald-500 border-y border-r border-y-zinc-800 border-r-zinc-800' 
          : 'bg-black border-l-transparent border-y border-r border-y-transparent border-r-transparent hover:bg-zinc-900 hover:border-l-zinc-700'
        }`}
    >
      <div className="p-3">
        <div className="flex justify-between items-start mb-2">
          <div className="flex-1 min-w-0 pr-2">
            <h4 className={`font-sans font-medium text-xs tracking-tight truncate ${isSelected ? 'text-white' : 'text-zinc-400 group-hover:text-zinc-200'}`}>
              {asset.address ? asset.address.split(',')[0] : 'Unknown Address'}
            </h4>
            <p className="text-zinc-600 text-[9px] font-mono uppercase truncate mt-0.5">
              {asset.property_type || 'Commercial Asset'}
            </p>
          </div>
          <Badge variant={riskVariant}>
            {asset.asset_rating_band || 'N/A'}
          </Badge>
        </div>

        <div className="grid grid-cols-2 gap-2 mt-2">
          <div className="bg-zinc-950 p-1.5 border border-zinc-900">
            <div className="text-[8px] text-zinc-600 font-mono uppercase mb-0.5">SIZE (SQM)</div>
            <div className="text-xs font-mono text-zinc-300">{asset.floor_area}</div>
          </div>
          <div className="bg-zinc-950 p-1.5 border border-zinc-900">
             <div className="text-[8px] text-zinc-600 font-mono uppercase mb-0.5">DISTRESS</div>
             <div className={`text-xs font-mono font-bold ${riskVariant === 'critical' ? 'text-red-500' : riskVariant === 'high' ? 'text-orange-500' : 'text-zinc-300'}`}>
               {riskScore}
             </div>
          </div>
        </div>
      </div>
    </button>
  );
};

// --- TERMINAL VIEW ---

const TerminalView = () => {
  const [searchTerm, setSearchTerm] = useState('');
  const [consoleLogs, setConsoleLogs] = useState([]);
  const [distressData, setDistressData] = useState([]);
  const [selectedAsset, setSelectedAsset] = useState(null);
  const [apiStatus, setApiStatus] = useState('connecting');

  // Load Data
  useEffect(() => {
    const fetchDistress = async () => {
      addLog("Initializing Secure Uplink...");
      try {
        const res = await fetch(`${API_URL}/api/distress-scan`);
        if (!res.ok) throw new Error("Connection Refused");
        const json = await res.json();
        const assets = json.data || [];
        setDistressData(assets);
        setApiStatus('connected');
        addLog(`✅ UPLINK SECURE. ${assets.length} ASSETS LOADED.`);
        if(assets.length > 0) setSelectedAsset(assets[0]);
      } catch (err) {
        setApiStatus('offline');
        addLog("❌ UPLINK FAILED. ENGAGING DEMO PROTOCOL.");
        const demoData = [
           { address: '1 Hollybush Place, E2', asset_rating_band: 'E', floor_area: 94, property_type: 'Warehouse', uprn: '10002341923', local_authority: 'E09000003' },
           { address: 'Unit 4b, Shoreditch', asset_rating_band: 'F', floor_area: 450, property_type: 'General Industrial', uprn: '10008257879', local_authority: 'E09000003' },
           { address: 'The Old Brewery, Brick Lane', asset_rating_band: 'G', floor_area: 1200, property_type: 'Retail/Leisure', uprn: '10001234567', local_authority: 'E09000003' },
           { address: '77 Whitechapel Rd', asset_rating_band: 'D', floor_area: 210, property_type: 'Office', uprn: '10009876543', local_authority: 'E09000003' },
        ];
        setDistressData(demoData);
        setSelectedAsset(demoData[0]);
      }
    };
    fetchDistress();
  }, []);

  const handleSearch = async (e) => {
    e.preventDefault();
    if (!searchTerm) return;
    addLog(`🔍 EXECUTING QUERY: "${searchTerm}"...`);
    try {
      const res = await fetch(`${API_URL}/api/search?q=${searchTerm}`);
      const json = await res.json();
      // API returns list directly or inside data key depending on endpoint logic
      const results = Array.isArray(json) ? json : json.data || []; 
      setDistressData(results);
      addLog(`✅ QUERY COMPLETE. ${results.length} RECORDS FOUND.`);
    } catch (err) {
      addLog("❌ QUERY FAILED. OFFLINE.");
    }
  };

  const addLog = (msg) => {
    setConsoleLogs(prev => [...prev.slice(-6), `[${new Date().toLocaleTimeString()}] ${msg}`]);
  };

  return (
    <div className="flex h-screen bg-black text-zinc-300 font-sans overflow-hidden">
      
      {/* 1. NARROW SIDEBAR (Navigation) */}
      <div className="w-14 border-r border-zinc-900 flex flex-col items-center py-4 bg-zinc-950">
        <div className="w-8 h-8 bg-emerald-500 rounded-sm mb-8 rotate-45 flex items-center justify-center">
            <div className="w-4 h-4 bg-black rotate-45"></div>
        </div>
        <div className="flex flex-col gap-6 w-full">
          <button className="h-10 w-full flex items-center justify-center text-zinc-400 hover:text-white hover:bg-zinc-900 transition-colors border-l-2 border-transparent hover:border-white"><LayoutGrid size={20} /></button>
          <button className="h-10 w-full flex items-center justify-center text-emerald-500 bg-zinc-900 border-l-2 border-emerald-500"><List size={20} /></button>
          <button className="h-10 w-full flex items-center justify-center text-zinc-400 hover:text-white hover:bg-zinc-900 transition-colors border-l-2 border-transparent hover:border-white"><MapIcon size={20} /></button>
          <button className="h-10 w-full flex items-center justify-center text-zinc-400 hover:text-white hover:bg-zinc-900 transition-colors border-l-2 border-transparent hover:border-white"><PieChart size={20} /></button>
        </div>
        <div className="mt-auto flex flex-col gap-6 w-full">
          <button className="h-10 w-full flex items-center justify-center text-zinc-400 hover:text-white hover:bg-zinc-900 transition-colors"><Bell size={20} /></button>
          <button className="h-10 w-full flex items-center justify-center text-zinc-400 hover:text-white hover:bg-zinc-900 transition-colors"><Settings size={20} /></button>
          <div className="h-10 w-full flex items-center justify-center pt-2 pb-4">
             <div className="w-8 h-8 rounded-full bg-zinc-800 flex items-center justify-center text-xs font-bold text-zinc-400 border border-zinc-700">JD</div>
          </div>
        </div>
      </div>

      {/* 2. LIST PANEL (The "Distress Cards") */}
      <div className="w-80 border-r border-zinc-900 flex flex-col bg-black">
        
        {/* Header */}
        <div className="h-14 border-b border-zinc-900 flex items-center justify-between px-4 bg-zinc-950/50">
          <h2 className="text-xs font-bold tracking-widest text-zinc-400 uppercase">Watchlist</h2>
          <div className="flex gap-2">
             <span className="text-[10px] font-mono text-emerald-500 bg-emerald-500/10 px-1.5 py-0.5 border border-emerald-500/20 rounded">
               {distressData.length} LIVE
             </span>
          </div>
        </div>

        {/* Search */}
        <div className="p-3 border-b border-zinc-900">
          <form onSubmit={handleSearch} className="relative group">
            <Search className="absolute left-3 top-2.5 text-zinc-600 group-focus-within:text-emerald-500 transition-colors" size={14} />
            <input 
              type="text" 
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              placeholder="SEARCH UPRN / POSTCODE..."
              className="w-full bg-zinc-900/50 border border-zinc-800 text-zinc-300 text-[10px] pl-9 pr-3 py-2.5 rounded-sm focus:border-emerald-900 focus:bg-zinc-900 focus:outline-none font-mono placeholder-zinc-700 transition-all uppercase"
            />
          </form>
        </div>

        {/* List */}
        <div className="flex-1 overflow-y-auto custom-scrollbar p-2 bg-black">
          {distressData.map((asset, i) => (
            <DistressCard 
              key={i} 
              asset={asset} 
              isSelected={selectedAsset === asset}
              onClick={() => setSelectedAsset(asset)}
            />
          ))}
        </div>

        {/* Mini Status Bar */}
        <div className="h-8 border-t border-zinc-900 flex items-center px-3 gap-2 bg-zinc-950">
           <div className={`w-1.5 h-1.5 rounded-full ${apiStatus === 'connected' ? 'bg-emerald-500 animate-pulse' : 'bg-red-500'}`}></div>
           <span className="text-[9px] font-mono text-zinc-500 uppercase tracking-wider">
             {apiStatus === 'connected' ? 'VANTAGE NET: SECURE' : 'VANTAGE NET: OFFLINE'}
           </span>
        </div>
      </div>

      {/* 3. MAIN DETAIL VIEW (The "Dossier") */}
      <div className="flex-1 bg-black relative flex flex-col min-w-0">
        {selectedAsset ? (
          <>
            {/* Dossier Header */}
            <div className="h-16 border-b border-zinc-900 flex items-center justify-between px-8 bg-zinc-950">
              <div className="flex flex-col justify-center">
                <div className="flex items-center gap-3 mb-1">
                    <h1 className="text-xl font-bold text-white tracking-tight font-sans">{selectedAsset.address}</h1>
                    <Badge variant={selectedAsset.asset_rating_band === 'F' || selectedAsset.asset_rating_band === 'G' ? 'critical' : 'neutral'}>
                    {selectedAsset.property_type}
                    </Badge>
                </div>
                <div className="flex items-center gap-4 text-[10px] font-mono text-zinc-500 uppercase">
                    <span>UPRN: <span className="text-zinc-300">{selectedAsset.uprn}</span></span>
                    <span className="text-zinc-700">|</span>
                    <span>AUTHORITY: <span className="text-zinc-300">{selectedAsset.local_authority || 'TOWER HAMLETS'}</span></span>
                </div>
              </div>
              
              <div className="flex gap-3">
                <button className="px-4 py-2 border border-zinc-700 hover:border-zinc-500 text-zinc-400 hover:text-white text-xs font-bold font-mono rounded-sm transition-colors flex items-center gap-2">
                  <FileText size={14}/> EXPORT PDF
                </button>
                <button className="px-4 py-2 bg-emerald-600 hover:bg-emerald-500 text-black text-xs font-bold font-mono rounded-sm transition-colors flex items-center gap-2 shadow-[0_0_15px_rgba(16,185,129,0.3)]">
                  <MousePointer2 size={14}/> ACQUIRE TARGET
                </button>
              </div>
            </div>

            <div className="flex-1 overflow-y-auto p-8 bg-black">
              
              {/* Top Metrics Grid */}
              <div className="grid grid-cols-4 gap-6 mb-8">
                {/* Metric 1 */}
                <div className="bg-zinc-950 border border-zinc-900 p-5 relative group hover:border-zinc-700 transition-colors">
                  <div className="text-[10px] font-mono text-zinc-500 uppercase mb-2 tracking-wider">Estimated Value</div>
                  <div className="text-2xl font-mono text-white tracking-tighter">£{(selectedAsset.floor_area * 2700).toLocaleString()}</div>
                  <div className="text-[10px] text-emerald-500 mt-2 flex items-center gap-1 font-mono">
                    <TrendingUp size={10} /> +12.4% vs REGION
                  </div>
                </div>
                
                {/* Metric 2: Distress Score */}
                <div className="bg-zinc-950 border border-zinc-900 p-5 relative overflow-hidden group hover:border-zinc-700 transition-colors">
                  <div className="absolute top-0 right-0 p-2 opacity-50">
                    <Activity size={16} className="text-red-500" />
                  </div>
                  <div className="text-[10px] font-mono text-zinc-500 uppercase mb-2 tracking-wider">Distress Score</div>
                  <div className="text-2xl font-mono text-white tracking-tighter">84<span className="text-sm text-zinc-600">/100</span></div>
                  <div className="w-full h-1 bg-zinc-800 mt-4 rounded-full overflow-hidden">
                    <div className="h-full bg-gradient-to-r from-orange-600 to-red-600 w-[84%]"></div>
                  </div>
                </div>

                {/* Metric 3: EPC */}
                <div className="bg-zinc-950 border border-zinc-900 p-5 group hover:border-zinc-700 transition-colors">
                  <div className="text-[10px] font-mono text-zinc-500 uppercase mb-2 tracking-wider">EPC Compliance</div>
                  <div className="flex items-end gap-2">
                      <div className={`text-2xl font-mono font-bold ${['F','G'].includes(selectedAsset.asset_rating_band) ? 'text-red-500' : 'text-white'}`}>
                        BAND {selectedAsset.asset_rating_band}
                      </div>
                      <div className="text-[10px] text-red-400 mb-1 font-mono">NON-COMPLIANT</div>
                  </div>
                  <div className="text-[10px] text-zinc-600 mt-2 font-mono">
                    MEES DEADLINE EXPIRED
                  </div>
                </div>

                {/* Metric 4: Occupancy */}
                <div className="bg-zinc-950 border border-zinc-900 p-5 group hover:border-zinc-700 transition-colors">
                  <div className="text-[10px] font-mono text-zinc-500 uppercase mb-2 tracking-wider">Est. Yield</div>
                  <div className="text-2xl font-mono text-white tracking-tighter">6.8%</div>
                  <div className="text-[10px] text-zinc-600 mt-2 font-mono">
                    BASED ON £25/SQFT ERV
                  </div>
                </div>
              </div>

              {/* Main Content Layout */}
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-8 h-[600px]">
                
                {/* Left Column: Risk & Insight */}
                <div className="lg:col-span-1 flex flex-col gap-6 h-full">
                  
                  {/* Risk Radar */}
                  <div className="bg-zinc-950 border border-zinc-900 p-6 flex-1 flex flex-col">
                    <div className="flex justify-between items-center mb-6">
                      <h3 className="text-xs font-bold text-zinc-300 font-mono uppercase tracking-wider flex items-center gap-2">
                        <AlertTriangle size={12} className="text-orange-500"/> Risk Matrix
                      </h3>
                    </div>
                    
                    <div className="space-y-5 flex-1">
                      {RISK_FACTORS.map((factor) => (
                        <div key={factor.name}>
                            <div className="flex justify-between items-end mb-1">
                                <span className="text-[10px] font-mono text-zinc-500 uppercase">{factor.name}</span>
                                <span className={`text-[10px] font-mono font-bold ${factor.color}`}>{factor.score}</span>
                            </div>
                            <div className="h-1.5 bg-zinc-900 rounded-sm overflow-hidden">
                                <div className={`h-full ${factor.bar}`} style={{width: `${factor.score * 10}%`}}></div>
                            </div>
                        </div>
                      ))}
                    </div>
                  </div>

                  {/* AI Insight */}
                  <div className="bg-zinc-950 border border-zinc-900 p-6 h-1/3">
                      <h4 className="text-[10px] font-bold text-emerald-500 font-mono uppercase mb-3 flex items-center gap-2">
                        <Wifi size={12} className="animate-pulse"/> VANTAGE AI
                      </h4>
                      <p className="text-xs text-zinc-400 leading-relaxed font-sans">
                        Asset flagged for <strong>Regulatory Obsolescence</strong>. Current owner (Shell Co) has not filed accounts in 14 months.
                        <br/><br/>
                        <span className="text-zinc-500">Recommendation:</span> Approach via debt holder (Octopus RE) for pre-market acquisition at ~20% discount to book value.
                      </p>
                  </div>

                </div>

                {/* Right Column: Map Context */}
                <div className="lg:col-span-2 bg-zinc-900 relative overflow-hidden border border-zinc-800 shadow-inner">
                  {/* Grid Pattern */}
                  <div className="absolute inset-0 bg-[linear-gradient(#27272a_1px,transparent_1px),linear-gradient(90deg,#27272a_1px,transparent_1px)] bg-[size:40px_40px] opacity-30"></div>
                  
                  {/* Central Marker */}
                  <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2">
                    <div className="relative">
                      <div className="w-32 h-32 border border-emerald-500/20 rounded-full absolute -top-16 -left-16 animate-ping opacity-20"></div>
                      <div className="w-4 h-4 bg-emerald-500 rounded-full border-4 border-black relative z-10 shadow-[0_0_20px_rgba(16,185,129,0.5)]"></div>
                      {/* Callout Line */}
                      <div className="absolute left-4 top-4 w-12 h-[1px] bg-emerald-500/50"></div>
                      <div className="absolute left-16 top-4 w-[1px] h-8 bg-emerald-500/50"></div>
                      <div className="absolute left-16 top-12 bg-black/80 border border-emerald-500/50 px-2 py-1 text-[8px] font-mono text-emerald-400 whitespace-nowrap backdrop-blur">
                        TARGET: {selectedAsset.uprn}
                      </div>
                    </div>
                  </div>
                  
                  {/* Overlay UI */}
                  <div className="absolute top-4 left-4 bg-black/80 backdrop-blur border border-zinc-800 px-3 py-2">
                    <div className="text-[8px] font-mono text-zinc-500 uppercase mb-1">SOURCE</div>
                    <div className="text-xs font-mono text-white flex items-center gap-2">
                        <div className="w-1.5 h-1.5 bg-orange-500 rounded-full"></div> OS MASTERMAP
                    </div>
                  </div>

                  <div className="absolute bottom-4 right-4 bg-black/80 backdrop-blur border border-zinc-800 px-3 py-2 flex gap-4">
                    <div>
                        <div className="text-[8px] font-mono text-zinc-500 uppercase mb-1">LATITUDE</div>
                        <div className="text-xs font-mono text-zinc-300">51.5284° N</div>
                    </div>
                    <div>
                        <div className="text-[8px] font-mono text-zinc-500 uppercase mb-1">LONGITUDE</div>
                        <div className="text-xs font-mono text-zinc-300">0.0592° W</div>
                    </div>
                  </div>
                </div>

              </div>

            </div>
          </>
        ) : (
          <div className="h-full flex flex-col items-center justify-center text-zinc-600 bg-zinc-950">
            <div className="w-20 h-20 border border-zinc-800 rounded-full flex items-center justify-center mb-6 bg-black">
              <Terminal size={32} className="opacity-50" />
            </div>
            <p className="font-mono text-xs tracking-widest uppercase mb-2">Awaiting Target Selection</p>
            <p className="font-sans text-[10px] text-zinc-700">Select an asset from the intelligence feed to initialize dossier.</p>
          </div>
        )}
      </div>

    </div>
  );
};

export default function App() {
  return (
    <TerminalView />
  );
}
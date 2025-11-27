import React, { useState, useEffect } from 'react';
import { 
  Search, Map as MapIcon, Zap, Database, Lock, 
  ArrowRight, Building2, Terminal, Activity, 
  Wifi, ServerCrash, MousePointer2, ChevronRight,
  LayoutGrid, List, PieChart, Bell, Settings, User,
  TrendingUp, AlertTriangle, FileText, Banknote, Users
} from 'lucide-react';

// --- CONFIGURATION ---
const API_URL = "http://localhost:8000";

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

const DistressCard = ({ asset, onClick, isSelected }) => {
  let riskVariant = 'low';
  let riskScore = '2.4';
  
  if (asset.asset_rating_band === 'F' || asset.asset_rating_band === 'G') {
    riskVariant = 'critical';
    riskScore = '9.1';
  } else if (asset.asset_rating_band === 'E') {
    riskVariant = 'high';
    riskScore = '7.5';
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
              {asset.company_name || asset.property_type || 'Commercial Asset'}
            </p>
          </div>
          <Badge variant={riskVariant}>
            {asset.asset_rating_band || 'N/A'}
          </Badge>
        </div>
      </div>
    </button>
  );
};

const TerminalView = () => {
  const [searchTerm, setSearchTerm] = useState('');
  const [distressData, setDistressData] = useState([]);
  const [selectedAsset, setSelectedAsset] = useState(null);
  const [corporateData, setCorporateData] = useState(null);
  const [chargesData, setChargesData] = useState([]);
  const [apiStatus, setApiStatus] = useState('connecting');
  const [loadingCorp, setLoadingCorp] = useState(false);

  // Load Data
  useEffect(() => {
    const fetchDistress = async () => {
      try {
        const res = await fetch(`${API_URL}/api/distress-scan`);
        if (!res.ok) throw new Error("Connection Refused");
        const json = await res.json();
        const assets = json.data || [];
        setDistressData(assets);
        setApiStatus('connected');
        if(assets.length > 0) setSelectedAsset(assets[0]);
      } catch (err) {
        setApiStatus('offline');
        // Fallback demo data
        const demoData = [
           { address: '311-313 WHITECHAPEL ROAD', asset_rating_band: 'G', floor_area: 1200, property_type: 'Retail', company_name: 'PLACES FOR LONDON LIMITED', company_number: '05557724' },
           { address: 'Unit 3, The Hanger', asset_rating_band: 'G', floor_area: 450, property_type: 'Industrial', company_name: 'GROUPADI LIMITED', company_number: '12345678' },
        ];
        setDistressData(demoData);
        setSelectedAsset(demoData[0]);
      }
    };
    fetchDistress();
  }, []);

  // Fetch Corporate Intelligence when asset changes
  useEffect(() => {
    if (selectedAsset && selectedAsset.company_number) {
      fetchCorporateIntel(selectedAsset.company_number);
    } else {
      setCorporateData(null);
      setChargesData([]);
    }
  }, [selectedAsset]);

  const fetchCorporateIntel = async (companyNumber) => {
    setLoadingCorp(true);
    try {
      // 1. Structure
      const structRes = await fetch(`${API_URL}/api/company/${companyNumber}/structure`);
      if (structRes.ok) {
        const structJson = await structRes.json();
        setCorporateData(structJson);
      }
      
      // 2. Charges (Debt)
      const chargeRes = await fetch(`${API_URL}/api/company/${companyNumber}/charges`);
      if (chargeRes.ok) {
        const chargeJson = await chargeRes.json();
        setChargesData(chargeJson.data || []);
      }
    } catch (e) {
      console.error("Intel Error", e);
    }
    setLoadingCorp(false);
  };

  const handleSearch = (e) => {
    e.preventDefault();
    // Implementation for search would go here
  };

  return (
    <div className="flex h-screen bg-black text-zinc-300 font-sans overflow-hidden">
      
      {/* 1. NARROW SIDEBAR */}
      <div className="w-14 border-r border-zinc-900 flex flex-col items-center py-4 bg-zinc-950">
        <div className="w-8 h-8 bg-emerald-500 rounded-sm mb-8 rotate-45 flex items-center justify-center">
            <div className="w-4 h-4 bg-black rotate-45"></div>
        </div>
        <div className="flex flex-col gap-6 w-full">
          <button className="h-10 w-full flex items-center justify-center text-emerald-500 bg-zinc-900 border-l-2 border-emerald-500"><LayoutGrid size={20} /></button>
          <button className="h-10 w-full flex items-center justify-center text-zinc-400 hover:text-white"><MapIcon size={20} /></button>
        </div>
      </div>

      {/* 2. LIST PANEL */}
      <div className="w-80 border-r border-zinc-900 flex flex-col bg-black">
        <div className="h-14 border-b border-zinc-900 flex items-center justify-between px-4 bg-zinc-950/50">
          <h2 className="text-xs font-bold tracking-widest text-zinc-400 uppercase">Distress Signal</h2>
          <span className="text-[10px] font-mono text-emerald-500 bg-emerald-500/10 px-1.5 py-0.5 border border-emerald-500/20 rounded">
            {distressData.length} LIVE
          </span>
        </div>
        <div className="p-3 border-b border-zinc-900">
          <form onSubmit={handleSearch} className="relative group">
            <Search className="absolute left-3 top-2.5 text-zinc-600" size={14} />
            <input 
              type="text" 
              placeholder="SEARCH ASSETS..."
              className="w-full bg-zinc-900/50 border border-zinc-800 text-zinc-300 text-[10px] pl-9 pr-3 py-2.5 rounded-sm focus:border-emerald-900 focus:bg-zinc-900 focus:outline-none font-mono placeholder-zinc-700 transition-all uppercase"
            />
          </form>
        </div>
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
      </div>

      {/* 3. MAIN DOSSIER VIEW */}
      <div className="flex-1 bg-black relative flex flex-col min-w-0">
        {selectedAsset ? (
          <>
            {/* Header */}
            <div className="h-16 border-b border-zinc-900 flex items-center justify-between px-8 bg-zinc-950">
              <div className="flex flex-col justify-center">
                <div className="flex items-center gap-3 mb-1">
                    <h1 className="text-xl font-bold text-white tracking-tight font-sans">{selectedAsset.address}</h1>
                    <Badge variant="critical">BAND {selectedAsset.asset_rating_band}</Badge>
                </div>
                <div className="flex items-center gap-4 text-[10px] font-mono text-zinc-500 uppercase">
                    <span>UPRN: <span className="text-zinc-300">{selectedAsset.uprn}</span></span>
                    <span className="text-zinc-700">|</span>
                    <span>OWNER: <span className="text-emerald-500 font-bold">{selectedAsset.company_name || 'UNKNOWN'}</span></span>
                </div>
              </div>
              <div className="flex gap-3">
                 <button className="px-4 py-2 border border-zinc-700 text-zinc-400 text-xs font-bold font-mono rounded-sm flex items-center gap-2">
                  <FileText size={14}/> EXPORT MEMO
                </button>
              </div>
            </div>

            <div className="flex-1 overflow-y-auto p-8 bg-black">
              
              {/* Top Row: Metrics */}
              <div className="grid grid-cols-3 gap-6 mb-8">
                <div className="bg-zinc-950 border border-zinc-900 p-5">
                  <div className="text-[10px] font-mono text-zinc-500 uppercase mb-2">Est. Value (Comps)</div>
                  <div className="text-2xl font-mono text-white tracking-tighter">£{(selectedAsset.floor_area * 350).toLocaleString()}</div>
                  <div className="text-[10px] text-zinc-600 mt-2 font-mono">BASED ON £350/SQFT (E1)</div>
                </div>
                <div className="bg-zinc-950 border border-zinc-900 p-5">
                  <div className="text-[10px] font-mono text-zinc-500 uppercase mb-2">Compliance Risk</div>
                  <div className="text-2xl font-mono text-red-500 tracking-tighter">CRITICAL</div>
                  <div className="text-[10px] text-red-900/50 mt-2 font-mono">UNLAWFUL TO LET (MEES)</div>
                </div>
                 <div className="bg-zinc-950 border border-zinc-900 p-5">
                  <div className="text-[10px] font-mono text-zinc-500 uppercase mb-2">Corporate Distress</div>
                  <div className="text-2xl font-mono text-orange-500 tracking-tighter">
                    {chargesData.length > 0 ? `${chargesData.length} CHARGES` : 'CLEAR'}
                  </div>
                  <div className="text-[10px] text-zinc-600 mt-2 font-mono">
                    {chargesData.length > 0 ? 'MATURING DEBT DETECTED' : 'NO CHARGES FILED'}
                  </div>
                </div>
              </div>

              {/* CORPORATE VEIL VISUALIZER */}
              <div className="grid grid-cols-2 gap-8 h-[500px]">
                
                {/* Left: Corporate Tree */}
                <div className="bg-zinc-950 border border-zinc-900 p-6 flex flex-col">
                  <h3 className="text-xs font-bold text-zinc-300 font-mono uppercase tracking-wider mb-6 flex items-center gap-2">
                    <Users size={14} className="text-emerald-500"/> Corporate Veil
                  </h3>
                  
                  {loadingCorp ? (
                    <div className="text-zinc-600 font-mono text-xs animate-pulse">Decrypting Corporate Structure...</div>
                  ) : corporateData ? (
                    <div className="relative flex-1">
                       {/* Tree Visualization (Simplified for React) */}
                       <div className="absolute left-8 top-0 bottom-0 w-[1px] bg-zinc-800"></div>
                       
                       <div className="space-y-8 relative z-10">
                          {/* 1. The Asset */}
                          <div className="flex items-center gap-4">
                             <div className="w-16 h-16 bg-zinc-900 border border-zinc-700 flex items-center justify-center rounded">
                                <Building2 size={24} className="text-zinc-500"/>
                             </div>
                             <div>
                                <div className="text-[10px] text-zinc-500 uppercase font-mono">ASSET</div>
                                <div className="text-sm text-white font-bold">{selectedAsset.address}</div>
                             </div>
                          </div>

                          {/* 2. The Company */}
                          <div className="flex items-center gap-4 ml-8">
                             <div className="w-4 h-[1px] bg-zinc-600"></div>
                             <div className="w-16 h-16 bg-emerald-900/20 border border-emerald-500/50 flex items-center justify-center rounded">
                                <BriefcaseIcon size={24} className="text-emerald-500"/>
                             </div>
                             <div>
                                <div className="text-[10px] text-zinc-500 uppercase font-mono">TITLE HOLDER</div>
                                <div className="text-sm text-white font-bold">{corporateData.profile?.company_name}</div>
                                <div className="text-[10px] text-zinc-500 font-mono">#{corporateData.profile?.company_number}</div>
                             </div>
                          </div>

                          {/* 3. The UBOs */}
                          <div className="ml-16 space-y-4">
                             {corporateData.beneficial_owners?.map((ubo, i) => (
                               <div key={i} className="flex items-center gap-4">
                                  <div className="w-4 h-[1px] bg-zinc-600"></div>
                                  <div className="w-12 h-12 bg-zinc-800 border border-zinc-600 flex items-center justify-center rounded-full">
                                      <User size={16} className="text-white"/>
                                  </div>
                                  <div>
                                      <div className="text-[10px] text-zinc-500 uppercase font-mono">BENEFICIAL OWNER</div>
                                      <div className="text-xs text-white font-bold">{ubo.name}</div>
                                      <div className="text-[9px] text-zinc-600 font-mono">{ubo.kind || 'INDIVIDUAL'}</div>
                                  </div>
                               </div>
                             ))}
                             {(!corporateData.beneficial_owners || corporateData.beneficial_owners.length === 0) && (
                               <div className="text-zinc-600 text-xs font-mono ml-8 italic">No PSC data available.</div>
                             )}
                          </div>
                       </div>
                    </div>
                  ) : (
                    <div className="text-zinc-600 font-mono text-xs">Select an asset with a corporate owner.</div>
                  )}
                </div>

                {/* Right: Debt Timeline */}
                <div className="bg-zinc-900 border border-zinc-800 p-6 overflow-y-auto">
                   <h3 className="text-xs font-bold text-zinc-300 font-mono uppercase tracking-wider mb-6 flex items-center gap-2">
                    <Banknote size={14} className="text-orange-500"/> Timeline of Squeeze (Charges)
                  </h3>
                  
                  {chargesData.length > 0 ? (
                    <div className="space-y-4">
                      {chargesData.map((charge, i) => (
                        <div key={i} className="bg-black border border-zinc-800 p-4 rounded-sm relative overflow-hidden">
                           <div className="flex justify-between items-start mb-2">
                              <span className="text-[10px] font-mono text-orange-500 border border-orange-500/30 px-1 rounded uppercase">
                                {charge.status}
                              </span>
                              <span className="text-[10px] font-mono text-zinc-500">
                                CREATED: {charge.created_on}
                              </span>
                           </div>
                           <div className="text-xs text-zinc-300 font-bold mb-1">
                              {charge.persons_entitled?.[0]?.name || 'UNKNOWN LENDER'}
                           </div>
                           <div className="text-[10px] text-zinc-600 font-mono truncate">
                              {charge.particulars?.description || 'Fixed Charge over Property'}
                           </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="flex flex-col items-center justify-center h-full text-zinc-600">
                       <Activity size={32} className="opacity-20 mb-2"/>
                       <p className="font-mono text-xs">No Outstanding Charges Found</p>
                    </div>
                  )}
                </div>

              </div>
            </div>
          </>
        ) : (
          <div className="flex items-center justify-center h-full text-zinc-500 font-mono text-xs">
            SELECT A TARGET TO INITIALIZE INTELLIGENCE
          </div>
        )}
      </div>

    </div>
  );
};

// Icon Helper
const BriefcaseIcon = ({size, className}) => (
  <svg xmlns="http://www.w3.org/2000/svg" width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={className}><rect width="20" height="14" x="2" y="7" rx="2" ry="2"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/></svg>
)

export default function App() {
  return <TerminalView />;
}
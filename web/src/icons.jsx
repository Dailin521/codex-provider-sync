import React from "react";

function Icon({ children, size = 18, className = "" }) {
  return (
    <svg className={className} width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      {children}
    </svg>
  );
}

export const RefreshIcon = (props) => <Icon {...props}><path d="M20 6v5h-5" /><path d="M4 18v-5h5" /><path d="M18.3 9A7 7 0 0 0 6.7 6.7L4 11" /><path d="M5.7 15A7 7 0 0 0 17.3 17.3L20 13" /></Icon>;
export const DatabaseIcon = (props) => <Icon {...props}><ellipse cx="12" cy="5" rx="8" ry="3" /><path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5" /><path d="M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6" /></Icon>;
export const HistoryIcon = (props) => <Icon {...props}><path d="M3 12a9 9 0 1 0 3-6.7L3 8" /><path d="M3 3v5h5" /><path d="M12 7v5l3 2" /></Icon>;
export const ActivityIcon = (props) => <Icon {...props}><path d="M3 12h4l2-7 4 14 2-7h6" /></Icon>;
export const OverviewIcon = (props) => <Icon {...props}><rect x="3" y="3" width="7" height="7" rx="1" /><rect x="14" y="3" width="7" height="7" rx="1" /><rect x="3" y="14" width="7" height="7" rx="1" /><rect x="14" y="14" width="7" height="7" rx="1" /></Icon>;
export const ShieldIcon = (props) => <Icon {...props}><path d="M12 3 4.5 6v5.3c0 4.6 3.2 8 7.5 9.7 4.3-1.7 7.5-5.1 7.5-9.7V6L12 3Z" /><path d="m9 12 2 2 4-4" /></Icon>;
export const ChevronIcon = (props) => <Icon {...props}><path d="m9 18 6-6-6-6" /></Icon>;
export const XIcon = (props) => <Icon {...props}><path d="m6 6 12 12M18 6 6 18" /></Icon>;
export const CheckIcon = (props) => <Icon {...props}><path d="m5 12 4 4L19 6" /></Icon>;
export const AlertIcon = (props) => <Icon {...props}><path d="M12 3 2.8 19h18.4L12 3Z" /><path d="M12 9v4M12 17h.01" /></Icon>;
export const FolderIcon = (props) => <Icon {...props}><path d="M3 6.5h6l2 2h10v9.5a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V6.5Z" /></Icon>;

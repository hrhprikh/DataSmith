"""
Log Analytics Dashboard
Provides visualization and analytics for log data including:
- Top attacker IPs
- Attack types per hour
- Status code heatmap
- Live anomalies detection
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import re
from typing import Dict, List, Any, Tuple
from collections import defaultdict, Counter
import ipaddress

class LogAnalyticsDashboard:
    def __init__(self):
        self.suspicious_patterns = {
            'sql_injection': [
                r"(?i)(union\s+select|drop\s+table|insert\s+into|delete\s+from)",
                r"(?i)('|\"|`).*(or|and)\s+\d+=\d+",
                r"(?i)(select.*from|where.*=.*--)"
            ],
            'xss_attempts': [
                r"(?i)<script[^>]*>.*</script>",
                r"(?i)javascript:",
                r"(?i)on(load|error|click|mouseover)\s*="
            ],
            'path_traversal': [
                r"\.\./",
                r"\.\.\\",
                r"%2e%2e%2f",
                r"%2e%2e\\"
            ],
            'command_injection': [
                r"(?i)(;|\||\&\&|\|\|).*(cat|ls|pwd|whoami|id|uname)",
                r"(?i)(system|exec|eval|cmd)\s*\(",
                r"(?i)\$\(.*\)"
            ],
            'brute_force': [
                r"(?i)(admin|administrator|root|test|guest)",
                r"(?i)(login|signin|auth).*attempt",
                r"(?i)(password|passwd|pwd).*fail"
            ]
        }
        
        self.anomaly_thresholds = {
            'requests_per_ip_per_minute': 100,
            'error_rate_threshold': 0.5,
            'unique_endpoints_per_ip': 50,
            'large_payload_size': 10000
        }

    def analyze_log_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Comprehensive analysis of log data
        """
        
        analytics = {
            'overview': self._get_overview_stats(df),
            'top_attackers': self._get_top_attacker_ips(df),
            'attack_timeline': self._get_attack_types_per_hour(df),
            'status_heatmap': self._get_status_code_heatmap(df),
            'anomalies': self._detect_live_anomalies(df),
            'geographical': self._analyze_ip_geography(df),
            'security_summary': self._generate_security_summary(df)
        }
        
        return analytics

    def _get_overview_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get basic overview statistics"""
        
        total_requests = len(df)
        unique_ips = df['ip_address'].nunique() if 'ip_address' in df.columns else 0
        
        # Time range
        if 'timestamp' in df.columns:
            time_range = {
                'start': df['timestamp'].min(),
                'end': df['timestamp'].max(),
                'duration_hours': (pd.to_datetime(df['timestamp'].max()) - 
                                 pd.to_datetime(df['timestamp'].min())).total_seconds() / 3600
            }
        else:
            time_range = {'start': None, 'end': None, 'duration_hours': 0}
        
        # Status code distribution
        status_dist = {}
        if 'status_code' in df.columns:
            status_dist = df['status_code'].value_counts().to_dict()
        
        # Request methods
        method_dist = {}
        if 'method' in df.columns:
            method_dist = df['method'].value_counts().to_dict()
        
        return {
            'total_requests': total_requests,
            'unique_ips': unique_ips,
            'time_range': time_range,
            'status_distribution': status_dist,
            'method_distribution': method_dist,
            'requests_per_hour': total_requests / max(time_range['duration_hours'], 1)
        }

    def _get_top_attacker_ips(self, df: pd.DataFrame, top_n: int = 20) -> List[Dict[str, Any]]:
        """Identify top attacker IPs based on suspicious activities"""
        
        if 'ip_address' not in df.columns:
            return []
        
        ip_stats = defaultdict(lambda: {
            'total_requests': 0,
            'error_requests': 0,
            'attack_attempts': 0,
            'attack_types': set(),
            'unique_endpoints': set(),
            'suspicious_patterns': [],
            'risk_score': 0
        })
        
        for _, row in df.iterrows():
            ip = row.get('ip_address', '')
            if not ip:
                continue
                
            stats = ip_stats[ip]
            stats['total_requests'] += 1
            
            # Track endpoints
            if 'endpoint' in row:
                stats['unique_endpoints'].add(row['endpoint'])
            
            # Check status codes for errors
            status = str(row.get('status_code', ''))
            if status.startswith('4') or status.startswith('5'):
                stats['error_requests'] += 1
            
            # Analyze request for attack patterns
            request_data = ' '.join([
                str(row.get('endpoint', '')),
                str(row.get('user_agent', '')),
                str(row.get('request_body', '')),
                str(row.get('query_params', ''))
            ]).lower()
            
            for attack_type, patterns in self.suspicious_patterns.items():
                for pattern in patterns:
                    if re.search(pattern, request_data):
                        stats['attack_attempts'] += 1
                        stats['attack_types'].add(attack_type)
                        stats['suspicious_patterns'].append({
                            'type': attack_type,
                            'pattern': pattern,
                            'matched_text': request_data[:100]
                        })
                        break
        
        # Calculate risk scores
        for ip, stats in ip_stats.items():
            risk_score = 0
            
            # High request volume
            if stats['total_requests'] > 1000:
                risk_score += 30
            elif stats['total_requests'] > 100:
                risk_score += 15
            
            # High error rate
            error_rate = stats['error_requests'] / max(stats['total_requests'], 1)
            if error_rate > 0.5:
                risk_score += 40
            elif error_rate > 0.2:
                risk_score += 20
            
            # Attack attempts
            risk_score += min(stats['attack_attempts'] * 5, 50)
            
            # Multiple attack types
            risk_score += len(stats['attack_types']) * 10
            
            # Too many unique endpoints (scanning behavior)
            if len(stats['unique_endpoints']) > 50:
                risk_score += 25
            
            stats['risk_score'] = min(risk_score, 100)
            stats['error_rate'] = error_rate
            stats['attack_types'] = list(stats['attack_types'])
            stats['unique_endpoints'] = len(stats['unique_endpoints'])
        
        # Sort by risk score and return top attackers
        top_attackers = sorted(
            [{'ip': ip, **stats} for ip, stats in ip_stats.items()],
            key=lambda x: x['risk_score'],
            reverse=True
        )[:top_n]
        
        return top_attackers

    def _get_attack_types_per_hour(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze attack types over time"""
        
        if 'timestamp' not in df.columns:
            return {'timeline': [], 'summary': {}}
        
        # Convert timestamp to datetime if needed
        df['datetime'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['datetime'].dt.floor('H')
        
        hourly_data = defaultdict(lambda: defaultdict(int))
        attack_summary = defaultdict(int)
        
        for _, row in df.iterrows():
            hour = row['hour']
            
            # Analyze request for attack patterns
            request_data = ' '.join([
                str(row.get('endpoint', '')),
                str(row.get('user_agent', '')),
                str(row.get('request_body', '')),
                str(row.get('query_params', ''))
            ]).lower()
            
            for attack_type, patterns in self.suspicious_patterns.items():
                for pattern in patterns:
                    if re.search(pattern, request_data):
                        hourly_data[hour][attack_type] += 1
                        attack_summary[attack_type] += 1
                        break
        
        # Format timeline data
        timeline = []
        for hour in sorted(hourly_data.keys()):
            hour_data = {
                'hour': hour.isoformat(),
                'total_attacks': sum(hourly_data[hour].values()),
                'attack_types': dict(hourly_data[hour])
            }
            timeline.append(hour_data)
        
        return {
            'timeline': timeline,
            'summary': dict(attack_summary),
            'peak_hour': max(timeline, key=lambda x: x['total_attacks']) if timeline else None
        }

    def _get_status_code_heatmap(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate status code heatmap data"""
        
        if 'status_code' not in df.columns or 'timestamp' not in df.columns:
            return {'heatmap_data': [], 'summary': {}}
        
        # Convert timestamp to datetime
        df['datetime'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['datetime'].dt.hour
        df['day_of_week'] = df['datetime'].dt.day_name()
        
        # Create heatmap data
        heatmap_data = []
        
        # Group by hour and day of week
        for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
            for hour in range(24):
                day_hour_data = df[
                    (df['day_of_week'] == day) & (df['hour'] == hour)
                ]
                
                if len(day_hour_data) > 0:
                    status_counts = day_hour_data['status_code'].value_counts().to_dict()
                    total_requests = len(day_hour_data)
                    
                    # Calculate error rate
                    error_codes = [code for code in status_counts.keys() 
                                 if str(code).startswith('4') or str(code).startswith('5')]
                    error_count = sum(status_counts.get(code, 0) for code in error_codes)
                    error_rate = error_count / total_requests if total_requests > 0 else 0
                    
                    heatmap_data.append({
                        'day': day,
                        'hour': hour,
                        'total_requests': total_requests,
                        'error_rate': error_rate,
                        'status_codes': status_counts,
                        'dominant_status': max(status_counts.items(), key=lambda x: x[1])[0]
                    })
                else:
                    heatmap_data.append({
                        'day': day,
                        'hour': hour,
                        'total_requests': 0,
                        'error_rate': 0,
                        'status_codes': {},
                        'dominant_status': None
                    })
        
        # Summary statistics
        summary = {
            'overall_error_rate': len(df[df['status_code'].astype(str).str.startswith(('4', '5'))]) / len(df),
            'peak_traffic_hour': df.groupby('hour').size().idxmax(),
            'peak_error_hour': df[df['status_code'].astype(str).str.startswith(('4', '5'))].groupby('hour').size().idxmax(),
            'status_distribution': df['status_code'].value_counts().to_dict()
        }
        
        return {
            'heatmap_data': heatmap_data,
            'summary': summary
        }

    def _detect_live_anomalies(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Detect various types of anomalies in the log data"""
        
        anomalies = []
        
        if len(df) == 0:
            return anomalies
        
        # 1. High request rate from single IP
        if 'ip_address' in df.columns:
            ip_counts = df['ip_address'].value_counts()
            high_volume_ips = ip_counts[ip_counts > self.anomaly_thresholds['requests_per_ip_per_minute']]
            
            for ip, count in high_volume_ips.items():
                anomalies.append({
                    'type': 'high_request_volume',
                    'severity': 'high' if count > 500 else 'medium',
                    'description': f"IP {ip} made {count} requests",
                    'ip_address': ip,
                    'request_count': int(count),
                    'threshold': self.anomaly_thresholds['requests_per_ip_per_minute']
                })
        
        # 2. Unusual error rates
        if 'status_code' in df.columns:
            error_requests = df[df['status_code'].astype(str).str.startswith(('4', '5'))]
            error_rate = len(error_requests) / len(df)
            
            if error_rate > self.anomaly_thresholds['error_rate_threshold']:
                anomalies.append({
                    'type': 'high_error_rate',
                    'severity': 'high',
                    'description': f"Error rate: {error_rate:.2%}",
                    'error_rate': error_rate,
                    'threshold': self.anomaly_thresholds['error_rate_threshold'],
                    'total_errors': len(error_requests)
                })
        
        # 3. Suspicious endpoint scanning
        if 'ip_address' in df.columns and 'endpoint' in df.columns:
            ip_endpoint_counts = df.groupby('ip_address')['endpoint'].nunique()
            scanning_ips = ip_endpoint_counts[
                ip_endpoint_counts > self.anomaly_thresholds['unique_endpoints_per_ip']
            ]
            
            for ip, endpoint_count in scanning_ips.items():
                anomalies.append({
                    'type': 'endpoint_scanning',
                    'severity': 'medium',
                    'description': f"IP {ip} accessed {endpoint_count} unique endpoints",
                    'ip_address': ip,
                    'unique_endpoints': int(endpoint_count),
                    'threshold': self.anomaly_thresholds['unique_endpoints_per_ip']
                })
        
        # 4. Large payload attacks
        if 'request_size' in df.columns:
            large_requests = df[df['request_size'] > self.anomaly_thresholds['large_payload_size']]
            
            for _, request in large_requests.iterrows():
                anomalies.append({
                    'type': 'large_payload',
                    'severity': 'medium',
                    'description': f"Large request: {request['request_size']} bytes",
                    'ip_address': request.get('ip_address', 'unknown'),
                    'request_size': int(request['request_size']),
                    'threshold': self.anomaly_thresholds['large_payload_size'],
                    'endpoint': request.get('endpoint', 'unknown')
                })
        
        # 5. Time-based anomalies
        if 'timestamp' in df.columns:
            df['datetime'] = pd.to_datetime(df['timestamp'])
            df['hour'] = df['datetime'].dt.hour
            
            # Unusual activity hours (late night/early morning spikes)
            hourly_counts = df['hour'].value_counts()
            night_hours = [0, 1, 2, 3, 4, 5]
            night_activity = hourly_counts.reindex(night_hours, fill_value=0).sum()
            total_activity = hourly_counts.sum()
            
            if night_activity > 0.3 * total_activity:  # More than 30% activity at night
                anomalies.append({
                    'type': 'unusual_time_activity',
                    'severity': 'low',
                    'description': f"High activity during night hours: {night_activity} requests",
                    'night_requests': int(night_activity),
                    'total_requests': int(total_activity),
                    'percentage': night_activity / total_activity
                })
        
        # Sort anomalies by severity
        severity_order = {'high': 3, 'medium': 2, 'low': 1}
        anomalies.sort(key=lambda x: severity_order.get(x['severity'], 0), reverse=True)
        
        return anomalies

    def _analyze_ip_geography(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze geographical distribution of IPs (basic analysis)"""
        
        if 'ip_address' not in df.columns:
            return {'countries': {}, 'suspicious_regions': []}
        
        # Basic IP analysis (without external GeoIP service)
        ip_analysis = {
            'total_unique_ips': df['ip_address'].nunique(),
            'private_ips': 0,
            'public_ips': 0,
            'suspicious_ips': []
        }
        
        for ip in df['ip_address'].unique():
            try:
                ip_obj = ipaddress.ip_address(ip)
                if ip_obj.is_private:
                    ip_analysis['private_ips'] += 1
                else:
                    ip_analysis['public_ips'] += 1
                    
                # Check for suspicious IP patterns
                if self._is_suspicious_ip(ip):
                    ip_analysis['suspicious_ips'].append(ip)
                    
            except ValueError:
                # Invalid IP address
                continue
        
        return ip_analysis

    def _is_suspicious_ip(self, ip: str) -> bool:
        """Basic suspicious IP detection"""
        
        suspicious_patterns = [
            # Known malicious ranges (basic examples)
            r'^192\.168\.1\.1$',  # Common default router (suspicious if external)
            r'^10\.0\.0\.1$',     # Common default router
            # Add more patterns as needed
        ]
        
        for pattern in suspicious_patterns:
            if re.match(pattern, ip):
                return True
        
        return False

    def _generate_security_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate overall security summary"""
        
        total_requests = len(df)
        if total_requests == 0:
            return {'risk_level': 'unknown', 'recommendations': []}
        
        # Calculate risk factors
        risk_factors = []
        risk_score = 0
        
        # Error rate risk
        if 'status_code' in df.columns:
            error_rate = len(df[df['status_code'].astype(str).str.startswith(('4', '5'))]) / total_requests
            if error_rate > 0.2:
                risk_factors.append(f"High error rate: {error_rate:.2%}")
                risk_score += 30
        
        # Attack attempts
        attack_count = 0
        for _, row in df.iterrows():
            request_data = ' '.join([
                str(row.get('endpoint', '')),
                str(row.get('user_agent', '')),
                str(row.get('request_body', ''))
            ]).lower()
            
            for patterns in self.suspicious_patterns.values():
                for pattern in patterns:
                    if re.search(pattern, request_data):
                        attack_count += 1
                        break
        
        if attack_count > 0:
            attack_rate = attack_count / total_requests
            risk_factors.append(f"Attack attempts detected: {attack_count}")
            risk_score += min(attack_rate * 100, 40)
        
        # Determine risk level
        if risk_score > 70:
            risk_level = 'critical'
        elif risk_score > 40:
            risk_level = 'high'
        elif risk_score > 20:
            risk_level = 'medium'
        else:
            risk_level = 'low'
        
        # Generate recommendations
        recommendations = []
        if error_rate > 0.2:
            recommendations.append("Investigate high error rates and potential application issues")
        if attack_count > 0:
            recommendations.append("Implement additional security measures to prevent attacks")
        if 'ip_address' in df.columns and df['ip_address'].nunique() > total_requests * 0.8:
            recommendations.append("High number of unique IPs - monitor for distributed attacks")
        
        return {
            'risk_level': risk_level,
            'risk_score': risk_score,
            'risk_factors': risk_factors,
            'recommendations': recommendations,
            'total_requests': total_requests,
            'attack_attempts': attack_count
        }

    def generate_dashboard_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate complete dashboard data"""
        
        print("🔍 Generating log analytics dashboard...")
        
        try:
            dashboard_data = self.analyze_log_data(df)
            
            # Add metadata
            dashboard_data['metadata'] = {
                'generated_at': datetime.now().isoformat(),
                'data_points': len(df),
                'analysis_version': '1.0',
                'columns_analyzed': list(df.columns)
            }
            
            print(f"✅ Dashboard generated with {len(df)} log entries")
            return dashboard_data
            
        except Exception as e:
            print(f"❌ Error generating dashboard: {e}")
            return {
                'error': str(e),
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'data_points': 0,
                    'analysis_version': '1.0'
                }
            }
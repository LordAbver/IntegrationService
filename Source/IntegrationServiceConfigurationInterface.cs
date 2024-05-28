using System;
using System.Configuration;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.ServiceModel;
using System.Text;

namespace Services.IntegrationService
{
    [ServiceBehavior(InstanceContextMode = InstanceContextMode.Single)]
    public class IntegrationServiceConfigurationInterface : ConfigurationInterface
    {
        public IntegrationServiceConfigurationInterface()
            : base(Config.Instance)
        {
            string folderPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().CodeBase);
            if (folderPath == null)
            {
                ServiceLogger.Warning("Integration Service was not able to get the " +
                                      "GetExecutingAssembly().CodeBase value. Starting with the default configuration values.");
            }
            else
            {
                string configurationFilePath;
                try
                {
                    configurationFilePath = ConfigurationManager.AppSettings["ConfigFilePath"];
                }
                catch (ConfigurationException)
                {
                    configurationFilePath = null;
                }

                if (configurationFilePath != null)
                    Config.Instance.Load(Path.Combine(folderPath, configurationFilePath));
                else
                {
                    ServiceLogger.Warning("Integration Service was not able to find the configuration file. " +
                                          "Starting with the default values.");
                }
            }
        }

        public override string GetConfiguration()
        {
            LoadRuleFileToConfigObject();
            return base.GetConfiguration();
        }

        public override void SetConfiguration(string config)
        {
            LoadRuleFileToConfigObject();
            var currentRuleFileHash = GetRuleFileHash();
            base.SetConfiguration(config);
            var newFileRuleHash = GetRuleFileHash();

            if (!CompareMd5Hash(currentRuleFileHash, newFileRuleHash))
                SaveRuleFileToFile();

            ClearRulesFileContents();
            Config.Instance.Save();
        }

        private void ClearRulesFileContents()
        {
            var param = Config.Instance.ConfigObject.ExternalInterfacesManagerParameters.CommunicationInterfacesParameters
                                                    .FirstOrDefault(item => item.Type == CommunicationInterfaceType.Bxf);
            if (param != null)
            {
                if (param.FormatterParameters is Bxf2008MessageFormatterParameters formatter)
                {
                    if (formatter.PlaylistTranslatorParameters is NativePlaylistTranslatorParameters cobj)
                    {
                        cobj.RulesFileContents = null;
                    }
                }
            }
        }

        private void LoadRuleFileToConfigObject()
        {
            var param = Config.Instance.ConfigObject.ExternalInterfacesManagerParameters.CommunicationInterfacesParameters
                                                    .FirstOrDefault(item => item.Type == CommunicationInterfaceType.Bxf);
            if (param != null)
            {
                if (param.FormatterParameters is Bxf2008MessageFormatterParameters formatter)
                {
                    if (formatter.PlaylistTranslatorParameters is NativePlaylistTranslatorParameters cobj && File.Exists(cobj.PathToRulesFile))
                    {
                        using (var inFile = File.OpenRead(cobj.PathToRulesFile))
                        {
                            using (var outData = new MemoryStream())
                            {
                                using (var compressStream = new GZipStream(outData, CompressionMode.Compress))
                                {
                                    inFile.CopyTo(compressStream);
                                }
                                cobj.RulesFileContents = outData.ToArray();
                            }
                        }
                    }
                }
            }
        }

        private void SaveRuleFileToFile()
        {
            var param = Config.Instance.ConfigObject.ExternalInterfacesManagerParameters.CommunicationInterfacesParameters
                                                    .FirstOrDefault(item => item.Type == CommunicationInterfaceType.Bxf);
            if (param != null)
            {
                if (param.FormatterParameters is Bxf2008MessageFormatterParameters formatter)
                {
                    if (formatter.PlaylistTranslatorParameters is NativePlaylistTranslatorParameters cobj)
                    {
                        if (string.IsNullOrWhiteSpace(cobj.PathToRulesFile))
                        {
                            string folderPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) ??
                                                string.Empty;
                            folderPath = Path.Combine(folderPath, "rules");
                            Directory.CreateDirectory(folderPath);
                            ServiceLogger.Debug("Created directory for the PlaylistTranslator rules file: " + folderPath);

                            cobj.PathToRulesFile = Path.Combine(folderPath, "PlaylistTranslator.Rule.xml");
                        }

                        using (var inData = new MemoryStream(cobj.RulesFileContents))
                        {
                            // Prevent overwriting the rules file with empty contents 
                            // when the operator saves the options of the Playlist Translator
                            if (inData.Length > 0)
                            {
                                string folderPath = Path.GetDirectoryName(cobj.PathToRulesFile);
                                if (folderPath != null && !Directory.Exists(folderPath))
                                {
                                    Directory.CreateDirectory(folderPath);
                                    ServiceLogger.Debug("Created directory for the PlaylistTranslator rules file before writing it: " + folderPath);
                                }

                                using (var outFile = new FileStream(cobj.PathToRulesFile, FileMode.Create, FileAccess.Write, FileShare.None))
                                {
                                    using (var decompressStream = new GZipStream(inData, CompressionMode.Decompress))
                                    {
                                        decompressStream.CopyTo(outFile);
                                    }
                                    ServiceLogger.DebugFormat("PlaylistTranslator rules file is written, its size {0} bytes", outFile.Length);
                                }
                            }
                        }
                    }
                }
            }
        }

        private string GetRuleFileHash()
        {
            byte[] ruleFileBytes = null;

            var param = Config.Instance.ConfigObject.ExternalInterfacesManagerParameters.CommunicationInterfacesParameters
                                                    .FirstOrDefault(item => item.Type == CommunicationInterfaceType.Bxf);
            if (param != null)
            {
                if (param.FormatterParameters is Bxf2008MessageFormatterParameters formatter)
                {
                    if (formatter.PlaylistTranslatorParameters is NativePlaylistTranslatorParameters cobj)
                    {
                        ruleFileBytes = cobj.RulesFileContents;
                    }
                }
            }

            if (ruleFileBytes != null)
            {
                return GetMd5Hash(ruleFileBytes);
            }
            return string.Empty;
        }

        private string GetMd5Hash(byte[] input)
        {
            var sBuilder = new StringBuilder();

            using (var md5Hash = MD5.Create())
            {
                var data = md5Hash.ComputeHash(input);
                for (int i = 0; i < data.Length; i++)
                {
                    sBuilder.Append(data[i].ToString("x2"));
                }
            }

            return sBuilder.ToString();
        }

        private static bool CompareMd5Hash(string hash1, string hash2)
        {
            var comparer = StringComparer.OrdinalIgnoreCase;
            return 0 == comparer.Compare(hash1, hash2);
        }
    }
}
